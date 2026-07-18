import { hashBytes } from "../core/content";
import { ByteBudget } from "../core/byte-budget";
import type { SyncCryptoService } from "../core/crypto-service";
import type { SyncTokenResponse } from "../remote/client";
import type { RemoteEntryState } from "../remote/changes";
import type { SyncPullClient } from "../remote/pull-client";
import type { SyncBlobStore } from "../store/ports";
import { isAutoMergeTextPath } from "./text-merge-policy";
import {
  DEFAULT_PREPARE_CONCURRENCY,
  type PlannedEntryState,
  type PreparedEntryBlob,
  requireBlobId,
} from "./pull-entry-state-internal";

// Total bytes of downloaded blob content a pull sub-window may hold in memory
// at once (see PreparePathBatchBlobsResult). Charged by encrypted size, which
// upper-bounds the retained plaintext.
export const DEFAULT_PULL_BLOB_BUDGET_BYTES = 48 * 1024 * 1024;
// Mobile WebViews die far earlier than desktop Electron under memory pressure
// (the iOS crash-loop incident), so hold much less there.
export const MOBILE_PULL_BLOB_BUDGET_BYTES = 24 * 1024 * 1024;
// Reserved against the budget for each in-flight download whose size is not
// yet known: blob sizes are not in the manifest and the HTTP layer buffers
// whole bodies, so the true size only appears once the download completes.
// The reservation is corrected to the actual size at that point.
export const DEFAULT_PULL_BLOB_PROVISIONAL_BYTES = 1 * 1024 * 1024;

interface PullBlobPreparerDeps {
  getApiBaseUrl: () => string;
  crypto: SyncCryptoService;
  pullClient: Pick<SyncPullClient, "downloadBlob">;
  prepareConcurrency?: number;
  // Notified (best-effort) when a blob was downloaded but permanently failed
  // verification (undecryptable ciphertext or content-hash mismatch) and was
  // quarantined. Never throws the pull.
  onDecryptFailure?: (entryId: string) => void;
}

export interface PreparePathBatchBlobsBudget {
  budget: ByteBudget;
  provisionalBytes: number;
  // Until the first download of a call settles, admit at most this many
  // download plans ("calibration"): the first observed actual size then
  // raises the provisional reservation for the rest of the call, so a window
  // of large media files degrades toward serial downloads instead of
  // buffering many multi-10MB bodies at once. Omit to disable (admit at full
  // concurrency from the start) — the right default on desktop, where the
  // transient download burst is survivable; set to 1 on mobile WebViews.
  calibrationConcurrency?: number;
}

export interface PreparePathBatchBlobsResult {
  blobs: PreparedEntryBlob[];
  // Whole path-dependency groups (a strict suffix of the given group order)
  // whose downloads were not admitted within the byte budget. The caller
  // applies + releases what WAS admitted, then runs another pass for these —
  // splitting only at group boundaries keeps rename/delete/write ordering
  // chains intact.
  remainder: PlannedEntryState[];
  // Releases every byte this call still holds against the budget. Idempotent;
  // must be called once the prepared blobs have been consumed (or abandoned).
  release: () => void;
}

/**
 * A blob that downloaded successfully (HTTP 200) but failed verification in a
 * way that is deterministic and permanent — the ciphertext will not decrypt, or
 * the decrypted content does not match the metadata hash. Retrying re-downloads
 * the identical bytes and fails identically, so a plain throw here would stall
 * the pull at this cursor and re-download the whole batch forever. These are
 * quarantined (skipped) instead. Transient failures (missing blob, network,
 * 5xx) are NOT this error — they still propagate so the window rolls back and
 * retries, which can succeed later.
 */
export class BlobVerificationError extends Error {
  constructor(
    readonly entryId: string,
    message: string,
  ) {
    super(message);
    this.name = "BlobVerificationError";
  }
}

export class PullBlobPreparer {
  constructor(private readonly deps: PullBlobPreparerDeps) {}

  /**
   * Downloads and verifies the blobs needed by the given path-dependency
   * groups, in group order, admitting new downloads only while the byte
   * budget allows.
   *
   * Admission rules:
   * - Groups are admitted atomically and in order; when a group's provisional
   *   reservations do not fit, admission stops and that group plus everything
   *   after it is returned as `remainder`.
   * - The first group is always admitted, whole, even if it exceeds the
   *   entire budget (run alone, never starve — guarantees forward progress).
   * - With `calibrationConcurrency` set, at most that many download plans are
   *   admitted until the first download of the call settles; the first
   *   observed actual size raises the provisional reservation for the rest of
   *   the call, so a window of large media files degrades toward serial
   *   downloads instead of buffering many multi-10MB bodies at once, while
   *   all-small windows regain full concurrency right after the first file.
   * - A completed download's reservation is corrected to its actual encrypted
   *   size. The bytes already exist at that point, so the correction may push
   *   the budget past its limit; the gate then simply stays closed until the
   *   caller releases.
   *
   * Without `budgetOptions` every group is admitted immediately (legacy
   * unbounded behavior) and `remainder` is always empty.
   */
  async preparePathBatchBlobs(
    store: SyncBlobStore,
    token: SyncTokenResponse,
    planGroups: ReadonlyArray<ReadonlyArray<PlannedEntryState>>,
    blobRequired: ReadonlySet<PlannedEntryState>,
    budgetOptions?: PreparePathBatchBlobsBudget,
  ): Promise<PreparePathBatchBlobsResult> {
    const blobPlansOf = (group: ReadonlyArray<PlannedEntryState>) =>
      group.filter(
        (plan) =>
          blobRequired.has(plan) &&
          plan.finalPath &&
          !plan.state.deleted &&
          plan.state.entryType !== "folder",
      );

    const blobs: PreparedEntryBlob[] = [];
    const releases: Array<() => void> = [];
    const provisionalByPlan = new Map<PlannedEntryState, () => void>();
    const releaseAll = () => {
      for (const release of releases) {
        release();
      }
      // Plans that were admitted (reserved) but never processed — e.g. the
      // call aborted on a transient error with work still queued. Releases
      // are idempotent, so double coverage is safe.
      for (const release of provisionalByPlan.values()) {
        release();
      }
      provisionalByPlan.clear();
    };
    const queue: PlannedEntryState[] = [];
    let groupIndex = 0;
    let admittedPlanCount = 0;
    let inFlight = 0;
    let calibrated = false;
    let maxActualBytesSeen = 0;
    let firstError: unknown = null;

    // Wakes workers parked while waiting for an in-flight completion to
    // unblock admission.
    let wakeWaiters: Array<() => void> = [];
    const wakeAll = () => {
      const waiters = wakeWaiters;
      wakeWaiters = [];
      for (const wake of waiters) {
        wake();
      }
    };
    const waitForWake = () =>
      new Promise<void>((resolve) => {
        wakeWaiters.push(resolve);
      });

    const effectiveProvisionalBytes = () =>
      Math.max(budgetOptions?.provisionalBytes ?? 0, maxActualBytesSeen);

    const calibrationLimit =
      budgetOptions?.calibrationConcurrency ?? Number.POSITIVE_INFINITY;
    const tryFeedGroups = (): void => {
      while (groupIndex < planGroups.length) {
        if (budgetOptions && !calibrated && admittedPlanCount >= calibrationLimit) {
          // Calibration: hold further admissions until the first actual size
          // is known.
          return;
        }
        const groupBlobPlans = blobPlansOf(planGroups[groupIndex]);
        if (budgetOptions && groupBlobPlans.length > 0) {
          const taken: Array<[PlannedEntryState, () => void]> = [];
          let admitted = true;
          for (const plan of groupBlobPlans) {
            let release = budgetOptions.budget.tryAcquire(
              effectiveProvisionalBytes(),
            );
            if (!release && admittedPlanCount === 0) {
              // The first group must always run, whole, even over budget.
              release = budgetOptions.budget.forceAcquire(
                effectiveProvisionalBytes(),
              );
            }
            if (!release) {
              admitted = false;
              break;
            }
            taken.push([plan, release]);
          }
          if (!admitted) {
            for (const [, release] of taken) {
              release();
            }
            return;
          }
          for (const [plan, release] of taken) {
            provisionalByPlan.set(plan, release);
          }
        }
        queue.push(...groupBlobPlans);
        admittedPlanCount += groupBlobPlans.length;
        groupIndex += 1;
      }
    };

    const processPlan = async (plan: PlannedEntryState): Promise<void> => {
      inFlight += 1;
      try {
        const verified = await this.downloadAndVerifyEntryBlob(store, token, plan);
        const provisionalRelease = provisionalByPlan.get(plan);
        provisionalByPlan.delete(plan);
        provisionalRelease?.();
        if (budgetOptions) {
          // The bytes exist now; charge their real (encrypted) size. This may
          // overshoot the budget — see the admission rules — and simply keeps
          // the gate closed until the caller releases.
          releases.push(
            budgetOptions.budget.forceAcquire(verified.encryptedByteLength),
          );
          maxActualBytesSeen = Math.max(
            maxActualBytesSeen,
            verified.encryptedByteLength,
          );
        }
        blobs.push({ plan, bytes: verified.bytes });
      } catch (error) {
        const provisionalRelease = provisionalByPlan.get(plan);
        provisionalByPlan.delete(plan);
        provisionalRelease?.();
        if (error instanceof BlobVerificationError) {
          // Permanent failure: quarantine this entry so the healthy entries in
          // the batch still apply and the cursor advances past the poison blob.
          this.deps.onDecryptFailure?.(error.entryId);
        } else {
          // Transient failure (missing blob, network, 5xx): preserve the
          // all-or-nothing rollback + retry behavior.
          firstError ??= error;
        }
      } finally {
        inFlight -= 1;
        calibrated = true;
        wakeAll();
      }
    };

    const workerCount = Math.max(
      1,
      this.deps.prepareConcurrency ?? DEFAULT_PREPARE_CONCURRENCY,
    );
    const workers = Array.from({ length: workerCount }, async () => {
      while (firstError === null) {
        if (queue.length === 0) {
          tryFeedGroups();
        }
        const plan = queue.shift();
        if (plan) {
          await processPlan(plan);
          continue;
        }
        if (inFlight === 0) {
          // Nothing running that could unblock admission: either every group
          // is admitted and drained, or the budget is full — remainder time.
          return;
        }
        await waitForWake();
      }
    });
    await Promise.all(workers);
    wakeAll();

    if (firstError !== null) {
      releaseAll();
      throw firstError;
    }

    return {
      blobs,
      remainder: planGroups.slice(groupIndex).flat(),
      release: releaseAll,
    };
  }

  private async downloadEntryBlob(
    token: SyncTokenResponse,
    state: RemoteEntryState,
  ): Promise<Uint8Array> {
    if (!state.blobId) {
      throw new Error(`Entry state ${state.entryId}@${state.revision} is missing a blob.`);
    }

    return await this.deps.pullClient.downloadBlob(
      this.deps.getApiBaseUrl(),
      token.token,
      token.vaultId,
      state.blobId,
    );
  }

  private async downloadAndVerifyEntryBlob(
    store: SyncBlobStore,
    token: SyncTokenResponse,
    plan: PlannedEntryState,
  ): Promise<{ bytes: Uint8Array; encryptedByteLength: number }> {
    const blobId = requireBlobId(plan.state);
    const encryptedBytes = await this.downloadEntryBlob(token, plan.state);
    let bytes: Uint8Array;
    try {
      bytes = await this.deps.crypto.decryptBlob(encryptedBytes, { blobId });
    } catch (error) {
      console.error(
        `[osync] pull: failed to decrypt blob ${blobId} for entry ${plan.state.entryId} rev=${plan.state.revision}`,
        error,
      );
      throw new BlobVerificationError(
        plan.state.entryId,
        `Entry state ${plan.state.entryId}@${plan.state.revision} blob could not be decrypted.`,
      );
    }
    const actualHash = await hashBytes(bytes);
    if (actualHash !== plan.hash) {
      throw new BlobVerificationError(
        plan.state.entryId,
        `Entry state ${plan.state.entryId}@${plan.state.revision} hash does not match metadata.`,
      );
    }
    if (plan.finalPath && isAutoMergeTextPath(plan.finalPath)) {
      await store.putBlob({
        blobId,
        hash: actualHash,
        encryptedBytes,
        role: "remote",
        refEntryId: plan.state.entryId,
        cachedAt: Date.now(),
      });
    }

    return { bytes, encryptedByteLength: encryptedBytes.byteLength };
  }
}
