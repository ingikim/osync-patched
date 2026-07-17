import { hashBytes } from "../core/content";
import type { SyncedEntryMetadata } from "../core/content";
import { getAvailableConflictCopyPath } from "../core/conflict-file";
import { isOperationError } from "../core/crypto";
import type {
  PendingMutationRow,
  SyncEntryStateRow,
} from "../store/store";
import {
  writeVaultBinary,
  writeVaultBytes,
} from "../vault/vault-writer";
import { decideConflictWinner } from "./conflict-tiebreak";
import {
  decodeUtf8,
  metadataContextFromPendingMutation,
  type PlannedEntryState,
  type PreparedEntryBlob,
  type PreparedPendingConflict,
  type PreparedPendingMerge,
} from "./pull-entry-state-internal";
import type {
  PullEntryStateApplierDeps,
  PullEntryStateStore,
  PullEntryStateVaultAdapter,
} from "./pull-entry-state-applier";
import { mergeText3 } from "./text-merge";
import { isAutoMergeTextPath } from "./text-merge-policy";

export class PullPendingMutationHandler {
  constructor(private readonly deps: PullEntryStateApplierDeps) {}

  async prepareConflictingPendingMutation(
    store: PullEntryStateStore,
    plan: PlannedEntryState,
    remoteBlob: PreparedEntryBlob | null,
  ): Promise<PreparedPendingConflict | null> {
    const pending = await this.findConflictingPendingMutation(store, plan);
    if (!pending) {
      return null;
    }

    let metadata: SyncedEntryMetadata;
    try {
      metadata = await this.deps.crypto.decryptMetadata(
        pending.encryptedMetadata,
        metadataContextFromPendingMutation(pending),
      );
    } catch (error) {
      if (!isOperationError(error)) {
        throw error;
      }
      // The pending row's metadata cannot be decrypted (corrupt/stale metadata
      // or baseRevision/AAD drift). Treat it as no-conflict so the pull cycle
      // is not aborted; the push-side self-heal re-seals or drops the row.
      console.error(
        `[osync] prepareConflictingPendingMutation: undecryptable pending mutation treated as no-conflict; remote state will be applied. mutationId=${pending.mutationId} entryId=${pending.entryId} op=${pending.op} blobId=${pending.blobId ?? "null"}`,
        error,
      );
      return null;
    }
    if (
      await isSameEntryPendingMutationAlreadyRemote(
        pending,
        metadata,
        plan,
        this.deps.vaultAdapter,
      )
    ) {
      return {
        plan,
        pending,
        event: null,
        conflictBytes: null,
        merge: { kind: "remote" },
      };
    }

    const entryState = await store.getEntryStateById(pending.entryId);
    const merge = await this.preparePendingTextMerge(
      store,
      plan,
      entryState,
      remoteBlob,
    );
    if (merge) {
      return {
        plan,
        pending,
        event: null,
        conflictBytes: null,
        merge,
      };
    }

    const winner = decideConflictWinner({
      serverEditedAt: plan.metadata.editedAt,
      serverUpdatedAt: plan.state.updatedAt,
      serverRevision: plan.state.revision,
      clientEditedAt: metadata.editedAt,
      clientRevision: pending.baseRevision + 1,
    });

    if (winner === "client") {
      let backupPath: string | null = null;
      let backupBytes: Uint8Array | null = null;
      if (
        remoteBlob &&
        plan.state.entryType !== "folder" &&
        plan.finalPath &&
        !plan.state.deleted
      ) {
        backupBytes = remoteBlob.bytes;
        backupPath = await getAvailableConflictCopyPath(
          this.deps.vaultAdapter,
          plan.finalPath,
          this.deps.now,
        );
      }
      return {
        plan,
        pending,
        event: {
          entryId: pending.entryId,
          op: pending.op,
          reason: "local_pending_mutation_wins" as const,
          originalPath: metadata.path,
          conflictPath: backupPath,
        },
        conflictBytes: backupBytes,
        merge: { kind: "rebase-client" },
      };
    }

    let conflictPath: string | null = null;
    let conflictBytes: Uint8Array | null = null;
    if (pending.op === "upsert" && (await this.deps.vaultAdapter.exists(metadata.path))) {
      conflictBytes = await this.deps.vaultAdapter.readBytes(metadata.path);
      conflictPath = await getAvailableConflictCopyPath(
        this.deps.vaultAdapter,
        metadata.path,
        this.deps.now,
      );
    }

    const event = {
      entryId: pending.entryId,
      op: pending.op,
      reason: "local_pending_mutation" as const,
      originalPath: metadata.path,
      conflictPath,
    };
    return {
      plan,
      pending,
      event,
      conflictBytes,
      merge: null,
    };
  }

  async applyPreparedPendingConflict(
    store: PullEntryStateStore,
    prepared: PreparedPendingConflict,
  ): Promise<void> {
    if (!prepared.event || prepared.merge) {
      // rebase-client and other merge cases handle conflict notification
      // themselves in applyPreparedPendingMerge.
      return;
    }

    if (prepared.event.conflictPath && prepared.conflictBytes) {
      await writeVaultBinary(
        this.deps.vaultAdapter,
        prepared.event.conflictPath,
        prepared.conflictBytes,
      );
    }

    await store.clearDirtyEntryByMutationId(prepared.pending.mutationId);
    this.deps.onConflict?.(prepared.event);
  }

  async applyPreparedPendingMerge(
    store: PullEntryStateStore,
    prepared: PreparedPendingConflict,
  ): Promise<void> {
    if (!prepared.merge) {
      return;
    }

    if (prepared.merge.kind === "remote") {
      await store.clearDirtyEntryByMutationId(prepared.pending.mutationId);
      return;
    }

    if (prepared.merge.kind === "rebase-client") {
      if (prepared.event?.conflictPath && prepared.conflictBytes) {
        await writeVaultBinary(
          this.deps.vaultAdapter,
          prepared.event.conflictPath,
          prepared.conflictBytes,
        );
      }
      const rebased: PendingMutationRow = {
        ...prepared.pending,
        mutationId: crypto.randomUUID(),
        baseRevision: prepared.plan.state.revision,
        baseBlobId: prepared.plan.state.blobId,
        baseHash: prepared.plan.hash,
      };
      // The metadata AAD is bound to {entryId, revision: baseRevision + 1, op,
      // blobId}. Changing baseRevision without re-encrypting would leave the
      // rebased row permanently undecryptable, so decrypt with the OLD context
      // and re-seal with the NEW one (mirrors the text-merge rebase path).
      try {
        const pendingMetadata = await this.deps.crypto.decryptMetadata(
          prepared.pending.encryptedMetadata,
          metadataContextFromPendingMutation(prepared.pending),
        );
        rebased.encryptedMetadata = await this.deps.crypto.encryptMetadata(
          pendingMetadata,
          {
            entryId: prepared.pending.entryId,
            revision: prepared.plan.state.revision + 1,
            op: prepared.pending.op,
            blobId: prepared.pending.blobId,
          },
        );
      } catch (error) {
        if (!isOperationError(error)) {
          throw error;
        }
        console.error(
          `[osync] rebase-client: re-seal failed, storing as-is mutationId=${rebased.mutationId} entryId=${rebased.entryId}`,
          error,
        );
      }
      await store.replaceDirtyEntry(rebased, { requireBaseBlob: false });
      if (prepared.event) {
        this.deps.onConflict?.(prepared.event);
      }
      return;
    }

    const rebasedMutation = {
      mutationId: crypto.randomUUID(),
      entryId: prepared.pending.entryId,
      op: "upsert" as const,
      baseRevision: prepared.plan.state.revision,
      baseBlobId: prepared.plan.state.blobId,
      baseHash: prepared.plan.hash,
      blobId: prepared.merge.blobId,
      hash: prepared.merge.hash,
      encryptedMetadata: prepared.merge.encryptedMetadata,
      createdAt: Date.now(),
    };
    await writeVaultBytes(this.deps.vaultAdapter, prepared.merge.path, prepared.merge.bytes);
    await store.replaceDirtyEntry(rebasedMutation, { requireBaseBlob: true });
    await store.applyLocalState({
      entryId: prepared.pending.entryId,
      path: prepared.merge.path,
      blobId: prepared.merge.blobId,
      hash: prepared.merge.hash,
      deleted: false,
      updatedAt: Date.now(),
      localMtime: null,
      localSize: null,
    });
  }

  private async preparePendingTextMerge(
    store: PullEntryStateStore,
    plan: PlannedEntryState,
    entryState: SyncEntryStateRow | null,
    remoteBlob: PreparedEntryBlob | null,
  ): Promise<PreparedPendingMerge | null> {
    const dirty = entryState?.dirty ?? null;
    const local = entryState?.local ?? null;
    const base = entryState?.base ?? null;
    if (
      dirty?.op !== "upsert" ||
      !entryState ||
      plan.state.deleted ||
      !plan.finalPath ||
      local?.path !== plan.finalPath ||
      !isAutoMergeTextPath(plan.finalPath) ||
      !base?.blobId ||
      !base.hash ||
      !remoteBlob ||
      !plan.hash
    ) {
      return null;
    }

    const cachedBase = await store.getBlob(base.blobId);
    if (!cachedBase || cachedBase.hash !== base.hash) {
      return null;
    }
    if (!(await this.deps.vaultAdapter.exists(local.path))) {
      return null;
    }

    const baseBytes = await this.deps.crypto.decryptBlob(
      cachedBase.encryptedBytes,
      { blobId: base.blobId },
    );
    const localBytes = await this.deps.vaultAdapter.readBytes(local.path);
    const baseText = decodeUtf8(baseBytes);
    const localText = decodeUtf8(localBytes);
    const remoteText = decodeUtf8(remoteBlob.bytes);
    if (baseText === null || localText === null || remoteText === null) {
      return null;
    }

    const merged = mergeText3(baseText, localText, remoteText);
    if (merged.status !== "clean") {
      return null;
    }

    const mergedBytes = new TextEncoder().encode(merged.text);
    const mergedHash = await hashBytes(mergedBytes);
    if (mergedHash === plan.hash) {
      return { kind: "remote" };
    }

    const blobId = crypto.randomUUID();
    return {
      kind: "local",
      bytes: mergedBytes,
      blobId,
      hash: mergedHash,
      path: plan.finalPath,
      encryptedMetadata: await this.deps.crypto.encryptMetadata(
        {
          path: plan.finalPath,
          hash: mergedHash,
        },
        {
          entryId: entryState.entryId,
          revision: plan.state.revision + 1,
          op: "upsert",
          blobId,
        },
      ),
    };
  }

  private async findConflictingPendingMutation(
    store: PullEntryStateStore,
    plan: PlannedEntryState,
  ): Promise<PendingMutationRow | null> {
    const entryMutation = await store.getDirtyEntryMutation(plan.state.entryId);
    if (entryMutation) {
      return entryMutation;
    }

    const candidatePaths = new Set(
      (plan.state.deleted
        ? [plan.metadata.path, plan.existing?.path]
        : [plan.finalPath, plan.existing?.path]
      ).filter((path): path is string => !!path),
    );
    if (candidatePaths.size === 0) {
      return null;
    }

    for (const pending of await store.listDirtyEntries()) {
      let metadata: SyncedEntryMetadata;
      try {
        metadata = await this.deps.crypto.decryptMetadata(
          pending.encryptedMetadata,
          metadataContextFromPendingMutation(pending),
        );
      } catch (error) {
        if (!isOperationError(error)) {
          throw error;
        }
        console.error(
          `[osync] findConflictingPendingMutation: undecryptable dirty entry skipped during path-conflict scan (corrupt/stale metadata or baseRevision/AAD drift); local pending changes for any colliding path may be overwritten by remote. mutationId=${pending.mutationId} entryId=${pending.entryId} op=${pending.op} blobId=${pending.blobId ?? "null"}`,
          error,
        );
        continue;
      }
      if (candidatePaths.has(metadata.path)) {
        return pending;
      }
    }

    return null;
  }
}

async function isSameEntryPendingMutationAlreadyRemote(
  pending: PendingMutationRow,
  metadata: { path: string; hash: string | null },
  plan: PlannedEntryState,
  vaultAdapter: PullEntryStateVaultAdapter,
): Promise<boolean> {
  if (pending.entryId !== plan.state.entryId) {
    return false;
  }

  if (pending.op === "delete") {
    return plan.state.deleted && metadata.path === plan.metadata.path;
  }

  if (
    plan.state.deleted ||
    metadata.path !== plan.finalPath ||
    metadata.hash === null ||
    metadata.hash !== plan.hash
  ) {
    return false;
  }

  if (!(await vaultAdapter.exists(metadata.path))) {
    return false;
  }

  return (await hashBytes(await vaultAdapter.readBytes(metadata.path))) === metadata.hash;
}
