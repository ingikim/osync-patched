import { afterEach, describe, expect, it, vi } from "vitest";

import { VaultKeyCryptoService } from "../../../core/crypto-service";
import { SyncPullService } from "../../pull-service";
import {
  createInitializedTestSyncStore,
  createTestPlugin,
} from "../../../../test-support/test-plugin";
import {
  createCommit,
  createPullClient,
  createRealtimeSession,
  createToken,
  createVaultAdapter,
  encryptPendingMetadata,
  encryptRemoteMetadata,
  encryptTestBlob,
  hashText,
  ignoreProgress,
  TEST_VAULT_KEY,
} from "./helpers";

const conflictTimestamp = () => new Date(2026, 3, 22, 10, 11, 12).getTime();

describe("SyncPullService dirty-entry scan poison isolation", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("skips an undecryptable dirty entry during the path-conflict scan and still finds conflicts for the healthy one", async () => {
    const plugin = createTestPlugin();
    const store = await createInitializedTestSyncStore(plugin);
    const localBody = "healthy local body";
    const remoteBody = "remote body";
    const localHash = await hashText(localBody);
    const remoteHash = await hashText(remoteBody);
    const adapter = createVaultAdapter({
      "Notes/collide.md": localBody,
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});

    // Poison dirty row: its metadata was sealed with baseRevision 7 but the
    // stored row says baseRevision 0 — the exact AAD drift produced when a
    // rebase changed baseRevision without re-encrypting. Decrypting it with
    // metadataContextFromPendingMutation throws an AES-GCM OperationError.
    // Ordered first (createdAt 1) so the scan must get PAST it.
    await store.markEntryDirty({
      mutationId: "mutation-poison",
      entryId: "entry-poison",
      op: "upsert",
      baseRevision: 0,
      blobId: "blob-poison",
      hash: await hashText("poison body"),
      encryptedMetadata: await encryptPendingMetadata({
        entryId: "entry-poison",
        baseRevision: 7,
        op: "upsert",
        blobId: "blob-poison",
        path: "Notes/poison-local.md",
        hash: await hashText("poison body"),
      }),
      createdAt: 1,
    });

    // Healthy dirty row for a DIFFERENT entry whose path collides with the
    // incoming remote entry, so only the listDirtyEntries scan can find it.
    await store.markEntryDirty({
      mutationId: "mutation-good",
      entryId: "entry-good",
      op: "upsert",
      baseRevision: 0,
      blobId: "blob-good-local",
      hash: localHash,
      encryptedMetadata: await encryptPendingMetadata({
        entryId: "entry-good",
        baseRevision: 0,
        op: "upsert",
        blobId: "blob-good-local",
        path: "Notes/collide.md",
        hash: localHash,
      }),
      createdAt: 5,
    });

    const conflicts: Array<{
      entryId: string;
      reason: string;
      originalPath: string;
      conflictPath: string | null;
    }> = [];
    const session = createRealtimeSession({
      pages: [
        {
          cursor: 2,
          hasMore: false,
          commits: [
            createCommit({
              cursor: 2,
              entryId: "entry-remote",
              revision: 1,
              blobId: "blob-remote",
              encryptedMetadata: await encryptRemoteMetadata({
                entryId: "entry-remote",
                revision: 1,
                blobId: "blob-remote",
                path: "Notes/collide.md",
                hash: remoteHash,
              }),
            }),
          ],
        },
      ],
    });
    const client = createPullClient({
      blobs: {
        "blob-remote": await encryptTestBlob(
          "blob-remote",
          new TextEncoder().encode(remoteBody),
        ),
      },
    });

    const service = new SyncPullService({
      getApiBaseUrl: () => "http://127.0.0.1:8787",
      getSyncToken: async () => createToken(),
      getSyncStore: () => store,
      crypto: new VaultKeyCryptoService(() => TEST_VAULT_KEY),
      vaultAdapter: adapter,
      pullClient: client,
      onProgress: ignoreProgress,
      onConflict(event) {
        conflicts.push({
          entryId: event.entryId,
          reason: event.reason,
          originalPath: event.originalPath,
          conflictPath: event.conflictPath,
        });
      },
      now: conflictTimestamp,
    });

    // One undecryptable dirty row must NOT abort the pull cycle.
    await expect(service.pullOnce(session)).resolves.toEqual({
      cursor: 2,
      entriesApplied: 1,
      filesWritten: 1,
      filesDeleted: 0,
      conflictsCreated: 1,
    });

    // The healthy colliding pending mutation is still detected: the remote
    // body is applied and the local content is preserved as a conflict copy.
    expect(adapter.text("Notes/collide.md")).toBe(remoteBody);
    expect(adapter.text("Notes/collide.sync-conflict-20260422-101112.md")).toBe(
      localBody,
    );
    expect(conflicts).toEqual([
      {
        entryId: "entry-good",
        reason: "local_pending_mutation",
        originalPath: "Notes/collide.md",
        conflictPath: "Notes/collide.sync-conflict-20260422-101112.md",
      },
    ]);

    // The poison row was skipped (with a diagnostic), not cleared or applied.
    const dirtyIds = (await store.listDirtyEntries()).map((row) => row.mutationId);
    expect(dirtyIds).toContain("mutation-poison");
    expect(dirtyIds).not.toContain("mutation-good");
    expect(
      consoleError.mock.calls.some((call) =>
        String(call[0]).includes(
          "undecryptable dirty entry skipped during path-conflict scan",
        ),
      ),
    ).toBe(true);

    await store.close();
  });
});
