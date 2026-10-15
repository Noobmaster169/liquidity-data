import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync, readdirSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openDuck, rowsJson } from '../src/duck.ts';
import { RangeWriter } from '../src/writer.ts';
import { schemaFor } from '../src/tables.ts';
import { decodeBlock } from '../src/decoders/block.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcBlock } from '../src/tx.ts';

function block(names: string[], blockTime: number | null): RpcBlock {
  return { blockTime, blockHeight: 1, parentSlot: 0, transactions: names.map((n) => loadFixture(n).tx) };
}

test('writes typed ZSTD parquet per table and date, sorted, with KV metadata', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  const ctx = buildContext();
  const conn = await openDuck(':memory:');
  const w = new RangeWriter({ dataRoot: root, conn, decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const d1 = decodeBlock(block(['jupiter_multihop_dlmm_swap2', 'humidifi_swap'], 1792085183), 100, ctx);
  const d2 = decodeBlock(block(['whirlpool_traded'], 1792085183 + 86400), 101, ctx);
  const tables = new Map(d1.tables);
  for (const [t, rows] of d2.tables) { const a = tables.get(t); if (a) a.push(...rows); else tables.set(t, rows); }
  const files = await w.writeRange('tail', 100, 101, tables);
  const swap = files.find((f) => f.table === 'events/meteora_dlmm/Swap2Evt')!;
  assert.equal(swap.path, 'events/meteora_dlmm/Swap2Evt/date=2026-10-15/100-101.parquet');
  assert.equal(swap.rows, 1);
  assert.equal(swap.sha256.length, 64);
  assert.ok(files.some((f) => f.path === 'blocks/date=2026-10-15/100-101.parquet'));
  assert.ok(files.some((f) => f.path === 'blocks/date=2026-10-16/100-101.parquet'));
  assert.ok(files.some((f) => f.path === 'events/orca_whirlpool/Traded/date=2026-10-16/100-101.parquet'));
  const rows = await rowsJson(conn, `SELECT slot, amount_in, fee_bps, swap_for_y, typeof(amount_in) AS t FROM read_parquet('${join(root, swap.path)}')`);
  assert.deepEqual(rows, [{ slot: '100', amount_in: '104023597531', fee_bps: '2145563', swap_for_y: true, t: 'UBIGINT' }]);
  const kv = await rowsJson(conn, `SELECT key, value FROM parquet_kv_metadata('${join(root, swap.path)}')`);
  assert.deepEqual(Object.fromEntries(kv.map((r) => [r.key, r.value])), { decoder_version: '1.0.0', idl_sha256: '', lane: 'tail', lo_slot: '100', hi_slot: '101' });
  assert.ok(!existsSync(join(root, '.staging')) || readdirSync(join(root, '.staging')).length === 0, 'staging cleaned');
  assert.ok(!readdirSync(join(root, 'events/meteora_dlmm/Swap2Evt/date=2026-10-15')).some((f) => f.endsWith('.tmp')));
});

test('null block_time rows go into the date of the nearest timed block in the range', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  const ctx = buildContext();
  const w = new RangeWriter({ dataRoot: root, conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const tables = new Map<string, Record<string, unknown>[]>([['blocks', [{ slot: 1, block_time: null, status: 'skipped' }, { slot: 2, block_time: '2026-10-15 00:00:00', status: 'ok' }]]]);
  const files = await w.writeRange('tail', 1, 2, tables);
  assert.deepEqual(files.map((f) => [f.path, f.rows]), [['blocks/date=2026-10-15/1-2.parquet', 2]]);
});

test('cleanupTemp removes leftover .tmp and staging files', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  mkdirSync(join(root, 'blocks/date=2026-10-15'), { recursive: true });
  mkdirSync(join(root, '.staging/tail'), { recursive: true });
  writeFileSync(join(root, 'blocks/date=2026-10-15/5-6.parquet.tail.tmp'), 'x');
  writeFileSync(join(root, '.staging/tail/5-6-blocks-2026-10-15.jsonl'), 'x');
  assert.equal(await RangeWriter.cleanupTemp(root, 'tail'), 2);
  assert.equal(readdirSync(join(root, 'blocks/date=2026-10-15')).length, 0);
});

test('temp files are scoped to a lane: cleaning tail leaves backfill staging alone', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  mkdirSync(join(root, 'blocks/date=2026-10-15'), { recursive: true });
  mkdirSync(join(root, '.staging/tail'), { recursive: true });
  mkdirSync(join(root, '.staging/backfill'), { recursive: true });
  writeFileSync(join(root, 'blocks/date=2026-10-15/5-6.parquet.tail.tmp'), 'x');
  writeFileSync(join(root, 'blocks/date=2026-10-15/1-2.parquet.backfill.tmp'), 'x');
  writeFileSync(join(root, '.staging/tail/a.jsonl'), 'x');
  writeFileSync(join(root, '.staging/backfill/b.jsonl'), 'x');
  assert.equal(await RangeWriter.cleanupTemp(root, 'tail'), 2);
  assert.ok(existsSync(join(root, 'blocks/date=2026-10-15/1-2.parquet.backfill.tmp')));
  assert.ok(existsSync(join(root, '.staging/backfill/b.jsonl')));
});

test('a range sink streams rows per block and produces the same files as writeRange', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fiber-'));
  const ctx = buildContext();
  const w = new RangeWriter({ dataRoot: root, conn: await openDuck(':memory:'), decoderVersion: '1.0.0', schemaFor: (t) => schemaFor(t, ctx) });
  const sink = w.openRange('tail', 100, 101);
  await sink.add(decodeBlock(block(['humidifi_swap'], 1792085183), 100, ctx).tables);
  await sink.add(decodeBlock(null, 101, ctx).tables);                       // skipped slot, null block_time
  const files = await sink.finish();
  const blocksFile = files.find((f) => f.table === 'blocks')!;
  assert.equal(blocksFile.path, 'blocks/date=2026-10-15/100-101.parquet');
  assert.equal(blocksFile.rows, 2);
  assert.equal(files.find((f) => f.table === 'swaps/humidifi')!.rows, 1);
  assert.ok(!existsSync(join(root, '.staging/tail')) || readdirSync(join(root, '.staging/tail')).length === 0);
});
