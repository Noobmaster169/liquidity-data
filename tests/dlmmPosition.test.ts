import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeDlmmPositions } from '../src/decoders/dlmmPosition.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('initialize_position args and named accounts become a row', () => {
  const out = decodeDlmmPositions(fixtureTx('dlmm_position_create'), buildContext());
  const r = out.rows.get('instructions/meteora_dlmm/initialize_position')![0]!;
  assert.equal(r.ix_path, '1');
  assert.equal(r.position, 'BPe1vLZSXvGbM4nn4SPE7PrkaPzb2yW2QJEeoMkVquXg');
  assert.equal(r.lb_pair, 'EQcKUXfKugCghU5kLe8vTsF4EnZZZJ2rPXou2RqzTN11');
  assert.equal(r.lower_bin_id, -469);
  assert.equal(r.width, 70);
  assert.equal(r.fee_owner, null);
  assert.equal(r.lock_release_point, null);
  assert.equal(r.trader, null, 'no transfers in a position creation');
});

test('a transaction without initialize_position yields nothing', () => {
  assert.equal(decodeDlmmPositions(fixtureTx('humidifi_swap'), buildContext()).rows.size, 0);
});
