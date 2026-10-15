import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runLane, processRange } from '../src/lane.ts';
import { Checkpoint } from '../src/checkpoint.ts';
import { RangeWriter } from '../src/writer.ts';
import { openDuck, rowsJson } from '../src/duck.ts';
import { schemaFor } from '../src/tables.ts';
import { inlineDecoder } from '../src/decodePool.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcClient, BlockResult } from '../src/rpc.ts';
import type { RpcBlock } from '../src/tx.ts';

function fakeRpc(tip: number, blocks: Map<number, RpcBlock | 'skipped'>, failOnce: Set<number> = new Set()): RpcClient {
  return {
    getSlot: async () => tip,
    getBlock: async (slot: number): Promise<BlockResult> => {
      if (failOnce.has(slot)) { failOnce.delete(slot); throw Object.assign(new Error('HTTP 429'), { retryable: true }); }
      const b = blocks.get(slot);
      if (b === undefined || b === 'skipped') return { kind: 'skipped' };
      return { kind: 'block', block: b };
    },
  } as unknown as RpcClient;
}
function blk(names: string[], t: number): RpcBlock { return { blockTime: t, blockHeight: 1, parentSlot: 0, transactions: names.map((n) => loadFixture(n).tx) }; }

async function setup() {
  const root = mkdtempSync(join(tmpdir(), 'fiber-lane-'));
  const ctx = buildContext();
  const checkpoint = await Checkpoint.open(join(root, 'state', 'tail.duckdb'));
  const writer = new RangeWriter({ dataRoot: join(root, 'data'), conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  return { root, ctx, checkpoint, writer };
}

test('tail processes ranges up to the tip margin, records skipped slots, commits and resumes', async () => {
  const { root, ctx, checkpoint, writer } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>([[100, blk(['humidifi_swap'], 1792085183)], [101, 'skipped'], [102, blk(['whirlpool_traded'], 1792085184)], [103, blk([], 1792085185)]]);
  const deps = { lane: 'tail' as const, rpc: fakeRpc(135, blocks), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 2, concurrency: 2, decoderVersion: '1.0.0', tipMargin: 32, fromSlot: 100, maxRanges: 2 };
  await runLane(deps);
  const st = await checkpoint.getState('tail');
  assert.equal(st?.nextSlot, 104);
  const ranges = await checkpoint.listRanges('tail');
  assert.deepEqual(ranges.map((r) => [r.loSlot, r.hiSlot]), [[100, 101], [102, 103]]);
  const conn = await openDuck(':memory:');
  const rows = await rowsJson(conn, `SELECT slot, status FROM read_parquet('${join(root, 'data', 'blocks', '*', '*.parquet')}') ORDER BY slot`);
  assert.deepEqual(rows.map((r) => [Number(r.slot), r.status]), [[100, 'ok'], [101, 'skipped'], [102, 'ok'], [103, 'ok']]);
  assert.ok(existsSync(join(root, 'data', 'swaps', 'humidifi')));
  await runLane({ ...deps, maxRanges: 0 });
  assert.equal((await checkpoint.listRanges('tail')).length, 2);
  await checkpoint.close();
});

test('a retryable fetch error inside a range is retried by the client; the lane never records it as skipped', async () => {
  const { ctx, checkpoint, writer } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>([[10, blk([], 1)], [11, blk([], 2)]]);
  const deps = { lane: 'tail' as const, rpc: fakeRpc(100, blocks, new Set([11])), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 2, concurrency: 2, decoderVersion: '1.0.0' };
  await assert.rejects(() => processRange(deps, 10, 11));
  assert.equal((await checkpoint.listRanges('tail')).length, 0);
  const files = await processRange(deps, 10, 11);
  assert.ok(files.some((f) => f.table === 'blocks' && f.rows === 2));
  await checkpoint.close();
});

test('backfill walks backward from fromSlot to targetSlot', async () => {
  const { checkpoint, writer, ctx } = await setup();
  const blocks = new Map<number, RpcBlock | 'skipped'>();
  for (let s = 90; s <= 99; s++) blocks.set(s, blk([], 1792085183 + s));
  await runLane({ lane: 'backfill', rpc: fakeRpc(1000, blocks), checkpoint, writer, decoder: inlineDecoder(ctx), rangeSlots: 4, concurrency: 4, decoderVersion: '1.0.0', fromSlot: 99, targetSlot: 90 });
  const ranges = await checkpoint.listRanges('backfill');
  assert.deepEqual(ranges.map((r) => [r.loSlot, r.hiSlot]).sort((a, b) => a[0]! - b[0]!), [[90, 91], [92, 95], [96, 99]]);
  assert.equal((await checkpoint.getState('backfill'))?.nextSlot, 89);
  await checkpoint.close();
});
