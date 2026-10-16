import type { RpcClient } from './rpc.ts';

/** First non-null block time at or after `slot` (probing forward over skipped slots). */
async function timeAtOrAfter(rpc: RpcClient, slot: number, limit: number): Promise<{ slot: number; time: number } | null> {
  for (let s = slot; s <= limit; s++) { const t = await rpc.getBlockTime(s); if (t !== null) return { slot: s, time: t }; }
  return null;
}

/**
 * Resolves `--until` to a slot. A number is a slot. A YYYY-MM-DD date is resolved by binary search on block times
 * below `startSlot`, returning the first slot whose block time is at or after midnight UTC of that date.
 * `<N>d` means N days before the block time of `startSlot`, so a fresh deployment can always ask for "90d".
 */
export async function resolveUntil(rpc: RpcClient, until: string, startSlot: number): Promise<number> {
  if (/^\d+$/.test(until)) return Number(until);
  const days = /^(\d+)d$/.exec(until);
  const dateSec = days ? NaN : Date.parse(until + 'T00:00:00Z') / 1000;
  if (!days && Number.isNaN(dateSec)) throw new Error(`--until must be a slot, YYYY-MM-DD or <N>d, got ${until}`);
  const startTime = (await timeAtOrAfter(rpc, startSlot, startSlot + 64))?.time;
  if (startTime === undefined) throw new Error(`cannot read block time near slot ${startSlot}`);
  const targetSec = days ? startTime - Number(days[1]) * 86400 : dateSec;
  if (targetSec >= startTime) return startSlot;
  // lower bound: slots are at least 0.3 s apart in practice, so this is far enough back
  let lo = Math.max(0, startSlot - Math.ceil((startTime - targetSec) / 0.3));
  let hi = startSlot;
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    const probe = await timeAtOrAfter(rpc, mid, hi);
    if (!probe) { hi = mid; continue; }
    if (probe.time >= targetSec) hi = probe.slot; else lo = probe.slot;
  }
  return hi;
}
