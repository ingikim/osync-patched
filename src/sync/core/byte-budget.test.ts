import { describe, expect, it } from "vitest";

import { ByteBudget } from "./byte-budget";

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

describe("ByteBudget", () => {
  it("never holds more than the budget for charges that fit it", async () => {
    const budget = new ByteBudget(100);
    const releases: Array<() => void> = [];
    const completed: number[] = [];

    // Three charges of B/2 against budget B: the third must wait until one of
    // the first two releases, so held never exceeds B.
    const acquisitions = [50, 50, 50].map(async (size, index) => {
      const release = await budget.acquire(size);
      releases.push(release);
      completed.push(index);
    });
    await tick();

    expect(completed).toEqual([0, 1]);
    expect(budget.heldBytes).toBe(100);
    expect(budget.peakHeldBytes).toBe(100);

    releases[0]();
    await Promise.all(acquisitions);

    expect(completed).toEqual([0, 1, 2]);
    expect(budget.peakHeldBytes).toBe(100);

    for (const release of releases) {
      release();
    }
    expect(budget.heldBytes).toBe(0);
  });

  it("admits an oversized charge once nothing else is held (runs alone, never starves)", async () => {
    const budget = new ByteBudget(100);
    const first = await budget.acquire(60);
    let oversizedAdmitted = false;
    const oversized = budget.acquire(200).then((release) => {
      oversizedAdmitted = true;
      return release;
    });
    await tick();
    expect(oversizedAdmitted).toBe(false);

    first();
    const release = await oversized;
    expect(oversizedAdmitted).toBe(true);
    expect(budget.heldBytes).toBe(200);

    release();
    expect(budget.heldBytes).toBe(0);
  });

  it("serves waiters in strict FIFO order so large waiters are not starved", async () => {
    const budget = new ByteBudget(100);
    const blocker = await budget.acquire(60);
    const order: string[] = [];
    const big = budget.acquire(80).then((release) => {
      order.push("big");
      return release;
    });
    // Would fit right now (60 + 10 <= 100) but must queue behind "big".
    const small = budget.acquire(10).then((release) => {
      order.push("small");
      return release;
    });
    await tick();
    expect(order).toEqual([]);

    blocker();
    const [releaseBig, releaseSmall] = await Promise.all([big, small]);
    expect(order).toEqual(["big", "small"]);

    releaseBig();
    releaseSmall();
    expect(budget.heldBytes).toBe(0);
  });

  it("tryAcquire admits only what fits now and yields to queued waiters", async () => {
    const budget = new ByteBudget(100);
    const first = budget.tryAcquire(70);
    expect(first).not.toBeNull();
    expect(budget.tryAcquire(40)).toBeNull();

    const waiting = budget.acquire(40);
    // A fitting charge is refused while a waiter is queued (FIFO fairness).
    expect(budget.tryAcquire(10)).toBeNull();

    first!();
    const release = await waiting;
    expect(budget.heldBytes).toBe(40);
    release();
  });

  it("forceAcquire may overshoot and releases are idempotent", () => {
    const budget = new ByteBudget(100);
    const release = budget.forceAcquire(250);
    expect(budget.heldBytes).toBe(250);
    expect(budget.peakHeldBytes).toBe(250);
    expect(budget.tryAcquire(1)).toBeNull();

    release();
    release();
    expect(budget.heldBytes).toBe(0);
    expect(budget.tryAcquire(1)).not.toBeNull();
  });
});
