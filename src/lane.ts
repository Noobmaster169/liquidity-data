import type { RpcClient } from './rpc.ts';
import type { Checkpoint } from './checkpoint.ts';
import { RangeWriter, type FileRecord } from './writer.ts';
import type { Decoder } from './decodePool.ts';
import { mergeTables } from './decoders/block.ts';
import type { Row } from './rows.ts';

export interface LaneDeps {
  lane: 'tail' | 'backfill'; rpc: RpcClient; checkpoint: Checkpoint; writer: RangeWriter; decoder: Decoder;
  rangeSlots: number; concurrency: number; decoderVersion: string;
  tipMargin?: number; targetSlot?: number; fromSlot?: number; maxRanges?: number; log?: (m: string) => void;
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => { for (;;) { const k = i++; if (k >= items.length) return; out[k] = await fn(items[k]!); } });
  await Promise.all(workers);
  return out;
}

export async function processRange(deps: LaneDeps, lo: number, hi: number): Promise<FileRecord[]> {
  const slots = Array.from({ length: hi - lo + 1 }, (_, k) => lo + k);
  const decoded = await mapLimit(slots, deps.concurrency, async (slot) => {
    const r = await deps.rpc.getBlock(slot);
    return deps.decoder.decode(r.kind === 'block' ? r.block : null, slot);
  });
  const tables = new Map<string, Row[]>();
  const stats: Record<string, number> = {};
  for (const d of decoded) { mergeTables(tables, d.tables); for (const [k, v] of Object.entries(d.stats)) stats[k] = (stats[k] ?? 0) + v; }
  const files = await deps.writer.writeRange(deps.lane, lo, hi, tables);
  deps.log?.(`${deps.lane} range ${lo}-${hi}: ${JSON.stringify(stats)} -> ${files.length} files`);
  return files;
}

export async function runLane(deps: LaneDeps): Promise<void> {
  const { lane, checkpoint, rangeSlots } = deps;
  let state = await checkpoint.getState(lane);
  if (!state) {
    if (lane === 'tail') {
      const start = deps.fromSlot ?? (await deps.rpc.getSlot()) - (deps.tipMargin ?? 32);
      state = { lane, startSlot: start, targetSlot: null, nextSlot: start };
    } else {
      if (deps.fromSlot === undefined || deps.targetSlot === undefined) throw new Error('backfill needs fromSlot and targetSlot on first start');
      state = { lane, startSlot: deps.fromSlot, targetSlot: deps.targetSlot, nextSlot: deps.fromSlot };
    }
    await checkpoint.initState(state);
  }
  let ranges = 0;
  for (;;) {
    if (deps.maxRanges !== undefined && ranges >= deps.maxRanges) return;
    let lo: number, hi: number, nextAfter: number;
    if (lane === 'tail') {
      const tip = (await deps.rpc.getSlot()) - (deps.tipMargin ?? 32);
      hi = Math.min(state.nextSlot + rangeSlots - 1, tip);
      lo = state.nextSlot;
      if (hi < lo) { await new Promise((r) => setTimeout(r, 2000)); continue; }
      nextAfter = hi + 1;
    } else {
      const target = state.targetSlot!;
      if (state.nextSlot < target) return;
      hi = state.nextSlot;
      lo = Math.max(hi - rangeSlots + 1, target);
      nextAfter = lo - 1;
    }
    const files = await processRange(deps, lo, hi);
    await checkpoint.commitRange(lane, lo, hi, files, deps.decoderVersion, nextAfter);
    state = { ...state, nextSlot: nextAfter };
    ranges++;
  }
}
