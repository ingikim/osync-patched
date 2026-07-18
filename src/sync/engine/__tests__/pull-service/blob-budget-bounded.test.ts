import { describe, expect, it } from "vitest";

import { ByteBudget } from "../../../core/byte-budget";
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
  encryptRemoteMetadata,
  encryptTestBlob,
  hashText,
  ignoreProgress,
  TEST_VAULT_KEY,
} from "./helpers";

describe("SyncPullService byte-budgeted blob pipeline", () => {
  it("applies a window of large blobs in bounded sub-windows without exceeding the budget", async () => {
    const plugin = createTestPlugin();
    const store = await createInitializedTestSyncStore(plugin);
    const adapter = createVaultAdapter();

    // Four "large" files (relative to the tiny test budget) plus one oversized
    // file bigger than the whole budget.
    const bodies = Array.from({ length: 4 }, (_, index) =>
      `file ${index + 1} `.repeat(200),
    );
    bodies.push(`oversized ${"x".repeat(12_000)}`);
    const encryptedBlobs = await Promise.all(
      bodies.map((body, index) =>
        encryptTestBlob(`blob-${index + 1}`, new TextEncoder().encode(body)),
      ),
    );
    const encryptedSizes = encryptedBlobs.map(
      (envelope) => new TextEncoder().encode(envelope).byteLength,
    );
    // Two normal blobs fit at once; the third does not. The oversized blob
    // exceeds the budget on its own and must still complete (alone).
    const budgetBytes = encryptedSizes[0] + encryptedSizes[1] + 200;
    expect(encryptedSizes[4]).toBeGreaterThan(budgetBytes);

    const commits = await Promise.all(
      bodies.map(async (body, index) =>
        createCommit({
          cursor: index + 1,
          entryId: `entry-${index + 1}`,
          revision: 1,
          blobId: `blob-${index + 1}`,
          encryptedMetadata: await encryptRemoteMetadata({
            entryId: `entry-${index + 1}`,
            revision: 1,
            blobId: `blob-${index + 1}`,
            path: `Media/file-${index + 1}.md`,
            hash: await hashText(body),
          }),
        }),
      ),
    );
    const session = createRealtimeSession({
      pages: [
        {
          cursor: bodies.length,
          hasMore: false,
          commits,
        },
      ],
    });

    const downloadedBlobIds: string[] = [];
    const inner = createPullClient({
      blobs: Object.fromEntries(
        encryptedBlobs.map((envelope, index) => [`blob-${index + 1}`, envelope]),
      ),
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
    };

    const blobByteBudget = new ByteBudget(budgetBytes);
    const service = new SyncPullService({
      getApiBaseUrl: () => "http://127.0.0.1:8787",
      getSyncToken: async () => createToken(),
      getSyncStore: () => store,
      crypto: new VaultKeyCryptoService(() => TEST_VAULT_KEY),
      vaultAdapter: adapter,
      pullClient: client,
      blobByteBudget,
      blobProvisionalBytes: 64,
      // Serialize the first download of each sub-window (the mobile setting)
      // so the first actual size calibrates admission — this is what makes
      // the retention bound strict for same-sized large files.
      blobCalibrationConcurrency: 1,
      onProgress: ignoreProgress,
    });

    const result = await service.pullOnce(session);

    // Every file is written correctly...
    expect(result.entriesApplied).toBe(bodies.length);
    expect(result.filesWritten).toBe(bodies.length);
    for (const [index, body] of bodies.entries()) {
      expect(adapter.text(`Media/file-${index + 1}.md`)).toBe(body);
    }
    // ...with each blob downloaded exactly once (sub-windowing re-plans but
    // never re-downloads)...
    expect([...downloadedBlobIds].sort()).toEqual(
      bodies.map((_, index) => `blob-${index + 1}`).sort(),
    );
    // ...while retention stayed bounded: never more than the budget, except
    // for the oversized blob which is allowed to run alone at its own size.
    const largestSingle = Math.max(...encryptedSizes);
    expect(blobByteBudget.peakHeldBytes).toBeLessThanOrEqual(
      Math.max(budgetBytes, largestSingle),
    );
    // The whole window was NOT retained at once (this is the incident fix).
    const totalBytes = encryptedSizes.reduce((sum, size) => sum + size, 0);
    expect(blobByteBudget.peakHeldBytes).toBeLessThan(totalBytes);
    // Everything was released at the end.
    expect(blobByteBudget.heldBytes).toBe(0);

    // Remote state recorded and cursor advanced for the whole window.
    for (let index = 0; index < bodies.length; index += 1) {
      expect(await store.getRemoteStateById(`entry-${index + 1}`)).toMatchObject({
        revision: 1,
        blobId: `blob-${index + 1}`,
      });
    }
    expect(result.cursor).toBe(bodies.length);
    expect(await store.getCursor()).toBe(bodies.length);

    await store.close();
  });
});
