/**
 * A byte-denominated admission gate for large in-memory payloads.
 *
 * Invariants:
 * - `tryAcquire`/`acquire` admit a charge only when nothing is currently held
 *   (so an entry larger than the whole budget can still run — alone, never
 *   starved) or when the charge fits inside `maxBytes`.
 * - Waiters are served in strict FIFO order: a small charge queued behind a
 *   large one waits, so the large one cannot be starved by a stream of small
 *   admissions.
 * - `forceAcquire` always succeeds and may push `heldBytes` past `maxBytes`.
 *   It exists for the moment a provisional reservation is corrected to the
 *   real size of bytes that ALREADY exist in memory: refusing the charge
 *   cannot un-allocate them, so the gate over-admits and simply stays closed
 *   to new admissions until enough is released.
 * - Every acquisition's release is idempotent.
 */
export class ByteBudget {
  private held = 0;
  private peak = 0;
  private readonly waiters: Array<{
    size: number;
    resolve: (release: () => void) => void;
  }> = [];

  constructor(readonly maxBytes: number) {
    if (!Number.isFinite(maxBytes) || maxBytes <= 0) {
      throw new Error(`ByteBudget requires a positive byte limit, got ${maxBytes}.`);
    }
  }

  get heldBytes(): number {
    return this.held;
  }

  /** Highest heldBytes ever observed; instrumentation for tests/diagnostics. */
  get peakHeldBytes(): number {
    return this.peak;
  }

  /**
   * Non-blocking admission. Returns a release function, or null when the
   * charge does not fit right now (or FIFO waiters are already queued).
   */
  tryAcquire(sizeBytes: number): (() => void) | null {
    const size = normalizeSize(sizeBytes);
    if (this.waiters.length > 0 || !this.canAdmit(size)) {
      return null;
    }
    return this.charge(size);
  }

  /** Blocking FIFO admission. */
  async acquire(sizeBytes: number): Promise<() => void> {
    const size = normalizeSize(sizeBytes);
    if (this.waiters.length === 0 && this.canAdmit(size)) {
      return this.charge(size);
    }
    return await new Promise<() => void>((resolve) => {
      this.waiters.push({ size, resolve });
    });
  }

  /**
   * Unconditional charge; may exceed maxBytes (see class docs). Prefer
   * tryAcquire/acquire wherever the bytes do not already exist in memory.
   */
  forceAcquire(sizeBytes: number): () => void {
    return this.charge(normalizeSize(sizeBytes));
  }

  private canAdmit(size: number): boolean {
    return this.held === 0 || this.held + size <= this.maxBytes;
  }

  private charge(size: number): () => void {
    this.held += size;
    this.peak = Math.max(this.peak, this.held);
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      this.held -= size;
      this.pump();
    };
  }

  private pump(): void {
    while (this.waiters.length > 0 && this.canAdmit(this.waiters[0].size)) {
      const waiter = this.waiters.shift()!;
      waiter.resolve(this.charge(waiter.size));
    }
  }
}

function normalizeSize(sizeBytes: number): number {
  if (!Number.isFinite(sizeBytes) || sizeBytes < 0) {
    return 0;
  }
  return sizeBytes;
}
