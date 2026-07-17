import { afterEach, describe, expect, it, vi } from "vitest";

import { VaultKeyCryptoService } from "../../../core/crypto-service";
import { hashBytes } from "../../../core/content";
import { decryptSyncMetadata } from "../../../core/crypto";
import type { CommitMutationPayload } from "../../../remote/realtime-client";
import {
  createInitializedTestSyncStore,
  createTestPlugin,
} from "../../../../test-support/test-plugin";
import { SyncPushService } from "../../push-service";
import {
  createPushSession,
  createToken,
  encryptMutationMetadata,
  ignoreProgress,
  metadataContextFromPayload,
  TEST_VAULT_KEY,
} from "./helpers";

// The pending row's metadata is sealed with a DIFFERENT baseRevision than the
// row stores, so decrypting with metadataContextFromMutation (revision =
// baseRevision + 1) fails the AES-GCM auth check with an OperationError —
// the exact drift produced when baseRevision moved without re-encrypting.
async function encryptDriftedMetadata(input: {
  entryId: string;
  blobId: string | null;
  path: string;
  hash: string;
}): Promise<string> {
  return await encryptMutationMetadata({
    entryId: input.entryId,
    baseRevision: 7,
    op: "upsert",
    blobId: input.blobId,
    path: input.path,
    hash: input.hash,
  });
}

describe("SyncPushService self-heal for undecryptable pending mutations", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("re-seals an undecryptable pending mutation from the local file and pushes it", async () => {
    const plugin = createTestPlugin();
    const store = await createInitializedTestSyncStore(plugin);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const body = "healed body";
    const bodyBytes = new TextEncoder().encode(body);
    const bodyHash = await hashBytes(bodyBytes);

    await store.applyLocalState({
      entryId: "entry-poison",
      path: "Folder/poison.md",
      blobId: "blob-poison",
      hash: bodyHash,
      deleted: false,
      updatedAt: 1,
      localMtime: null,
      localSize: null,
    });
    await store.markEntryDirty({
      mutationId: "mutation-poison",
      entryId: "entry-poison",
      op: "upsert",
      baseRevision: 0,
      blobId: "blob-poison",
      hash: bodyHash,
      encryptedMetadata: await encryptDriftedMetadata({
        entryId: "entry-poison",
        blobId: "blob-poison",
        path: "Folder/poison.md",
        hash: bodyHash,
      }),
      createdAt: 1,
    });

    const committed: CommitMutationPayload[] = [];
    let nextCursor = 10;
    const session = createPushSession(async (mutation) => {
      committed.push(mutation);
      nextCursor += 1;
      return {
        cursor: nextCursor,
        entryId: mutation.entryId,
        revision: mutation.baseRevision + 1,
      };
    });
    const service = new SyncPushService({
      getApiBaseUrl: () => "http://127.0.0.1:8787",
      getSyncToken: async () => createToken(),
      getSyncStore: () => store,
      crypto: new VaultKeyCryptoService(() => TEST_VAULT_KEY),
      fileReader: {
        async readBytes(path) {
          if (path === "Folder/poison.md") {
            return bodyBytes;
          }
          throw new Error(`unexpected read for ${path}`);
        },
      },
      blobClient: {
        async uploadBlob() {},
      },
      onProgress: ignoreProgress,
    });

    const result = await service.pushPendingMutations(session);

    // Drain 1: the undecryptable row is requeued as a fresh upsert.
    // Drain 2: the re-sealed mutation pushes successfully.
    expect(result).toMatchObject({
      mutationsPushed: 1,
      mutationsRequeued: 1,
      filesCreatedOrUpdated: 1,
      shouldPullAfterPush: false,
      hasMore: false,
    });
    expect(committed).toHaveLength(1);
    expect(committed[0]).toMatchObject({
      entryId: "entry-poison",
      op: "upsert",
    });
    // The committed metadata decrypts with the payload's own AAD context.
    await expect(
      decryptSyncMetadata(
        TEST_VAULT_KEY,
        committed[0]?.encryptedMetadata ?? "",
        metadataContextFromPayload(committed[0]),
      ),
    ).resolves.toMatchObject({
      path: "Folder/poison.md",
      hash: bodyHash,
    });
    expect(await store.listDirtyEntries()).toEqual([]);
    expect(
      consoleError.mock.calls.some((call) =>
        String(call[0]).includes("re-sealing undecryptable pending mutation"),
      ),
    ).toBe(true);

    await store.close();
  });

  it("drops an undecryptable pending mutation when no local file exists", async () => {
    const plugin = createTestPlugin();
    const store = await createInitializedTestSyncStore(plugin);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const bodyHash = await hashBytes(new TextEncoder().encode("gone body"));

    // No local state at all for this entry — nothing to re-seal from.
    await store.markEntryDirty({
      mutationId: "mutation-orphan",
      entryId: "entry-orphan",
      op: "upsert",
      baseRevision: 0,
      blobId: "blob-orphan",
      hash: bodyHash,
      encryptedMetadata: await encryptDriftedMetadata({
        entryId: "entry-orphan",
        blobId: "blob-orphan",
        path: "Folder/orphan.md",
        hash: bodyHash,
      }),
      createdAt: 1,
    });

    const session = createPushSession(async () => {
      throw new Error("nothing should be committed");
    });
    const service = new SyncPushService({
      getApiBaseUrl: () => "http://127.0.0.1:8787",
      getSyncToken: async () => createToken(),
      getSyncStore: () => store,
      crypto: new VaultKeyCryptoService(() => TEST_VAULT_KEY),
      fileReader: {
        async readBytes(path) {
          throw new Error(`unexpected read for ${path}`);
        },
      },
      blobClient: {
        async uploadBlob() {
          throw new Error("nothing should be uploaded");
        },
      },
      onProgress: ignoreProgress,
    });

    const result = await service.pushPendingMutations(session);

    expect(result).toMatchObject({
      mutationsPushed: 0,
      mutationsRequeued: 1,
      filesCreatedOrUpdated: 0,
      hasMore: false,
    });
    expect(await store.listDirtyEntries()).toEqual([]);
    expect(
      consoleError.mock.calls.some((call) =>
        String(call[0]).includes(
          "dropping undecryptable pending mutation mutation-orphan",
        ),
      ),
    ).toBe(true);

    await store.close();
  });
});
