import { describe, expect, it } from "vitest";

import { VaultKeyCryptoService } from "../../../core/crypto-service";
import { DEFAULT_SYNC_FILE_RULES } from "../../../core/file-rules";
import type { SyncPullClient } from "../../../remote/pull-client";
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
  encryptRemoteMetadata,
  encryptTestBlob,
  hashText,
  ignoreProgress,
  TEST_VAULT_KEY,
} from "./helpers";

describe("SyncPullService excluded-path blob preparation", () => {
  it("never downloads blobs for excluded paths but still records their remote state", async () => {
    const plugin = createTestPlugin();
    const store = await createInitializedTestSyncStore(plugin);
    const adapter = createVaultAdapter();
    const noteBody = "note body";
    const noteHash = await hashText(noteBody);
    // Stand-in for the production incident: a large audio blob under an
    // excluded folder in the same manifest window as a small healthy note.
    const audioHash = await hashText("giant ogg payload");

    const session = createRealtimeSession({
      pages: [
        {
          cursor: 2,
          hasMore: false,
          commits: [
            createCommit({
              cursor: 1,
              entryId: "entry-audio",
              revision: 1,
              blobId: "blob-audio",
              encryptedMetadata: await encryptRemoteMetadata({
                entryId: "entry-audio",
                revision: 1,
                blobId: "blob-audio",
                path: "Hyprnote/audio/recording.ogg",
                hash: audioHash,
              }),
            }),
            createCommit({
              cursor: 2,
              entryId: "entry-note",
              revision: 1,
              blobId: "blob-note",
              encryptedMetadata: await encryptRemoteMetadata({
                entryId: "entry-note",
                revision: 1,
                blobId: "blob-note",
                path: "Notes/note.md",
                hash: noteHash,
              }),
            }),
          ],
        },
      ],
    });

    // Only the included note has a blob fixture. If the excluded audio blob
    // were still being prepared (the pre-fix behavior), downloadBlob would
    // throw "missing blob fixture for blob-audio" and the pull would fail.
    const downloadedBlobIds: string[] = [];
    const inner = createPullClient({
      blobs: {
        "blob-note": await encryptTestBlob(
          "blob-note",
          new TextEncoder().encode(noteBody),
        ),
      },
    });
    const client = {
      async downloadBlob(
        apiBaseUrl: string,
        syncToken: string,
        vaultId: string,
        blobId: string,
      ): Promise<Uint8Array> {
        downloadedBlobIds.push(blobId);
        return await inner.downloadBlob(apiBaseUrl, syncToken, vaultId, blobId);
      },
    } as SyncPullClient;

    const service = new SyncPullService({
      getApiBaseUrl: () => "http://127.0.0.1:8787",
      getSyncToken: async () => createToken(),
      getSyncStore: () => store,
      crypto: new VaultKeyCryptoService(() => TEST_VAULT_KEY),
      vaultAdapter: adapter,
      pullClient: client,
      getSyncFileRules: () => ({
        ...DEFAULT_SYNC_FILE_RULES,
        excludedFolders: ["Hyprnote/audio"],
      }),
      onProgress: ignoreProgress,
    });

    const result = await service.pullOnce(session);

    // Blob preparation ran ONLY for the included entry.
    expect(downloadedBlobIds).toEqual(["blob-note"]);

    // The included entry is written normally.
    expect(adapter.text("Notes/note.md")).toBe(noteBody);
    // The excluded entry never touches the disk.
    expect(adapter.files.has("Hyprnote/audio/recording.ogg")).toBe(false);
    expect(adapter.files.size).toBe(1);

    // The excluded entry's remote state IS recorded and the cursor advances,
    // so the window completes and the entry is never re-fetched — this is what
    // breaks the iOS re-download crash loop.
    const remote = await store.getRemoteStateById("entry-audio");
    expect(remote).toMatchObject({
      revision: 1,
      blobId: "blob-audio",
      hash: audioHash,
    });
    expect(result.cursor).toBe(2);
    expect(await store.getCursor()).toBe(2);

    await store.close();
  });
});
