import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Checkpoint } from '../src/checkpoint.ts';

test('state init, range commit and resume', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'fiber-ckpt-'));
  const path = join(dir, 'tail.duckdb');
  let c = await Checkpoint.open(path);
  assert.equal(await c.getState('tail'), null);
  await c.initState({ lane: 'tail', startSlot: 100, targetSlot: null, nextSlot: 100 });
  await c.commitRange('tail', 100, 104, [{ table: 'blocks', path: 'blocks/date=2026-10-15/100-104.parquet', rows: 5, sha256: 'ab' }], '1.0.0', 105);
  await c.close();
  c = await Checkpoint.open(path);
  assert.deepEqual(await c.getState('tail'), { lane: 'tail', startSlot: 100, targetSlot: null, nextSlot: 105 });
  const ranges = await c.listRanges('tail');
  assert.equal(ranges.length, 1);
  assert.equal(ranges[0]!.loSlot, 100);
  assert.equal(ranges[0]!.hiSlot, 104);
  assert.deepEqual(ranges[0]!.files[0]!.path, 'blocks/date=2026-10-15/100-104.parquet');
  await c.close();
});
