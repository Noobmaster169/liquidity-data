import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeBlock } from '../src/decoders/block.ts';
import { buildContext, loadFixture } from './helpers.ts';
import type { RpcBlock } from '../src/tx.ts';

function blockOf(names: string[]): RpcBlock {
  const fx = names.map(loadFixture);
  return { blockTime: fx[0]!.blockTime, blockHeight: 1, parentSlot: 457320275, transactions: fx.map((f) => f.tx) };
}

test('a synthetic block of fixtures yields the expected tables and a blocks row', () => {
  const b = blockOf(['vote', 'failed_venue_tx', 'jupiter_multihop_dlmm_swap2', 'humidifi_swap', 'dflow_route_with_fee_leg', 'propamm_oracle_update_no_transfers', 'raydium_clmm_swap']);
  const d = decodeBlock(b, 457320276, buildContext());
  assert.deepEqual(d.tables.get('blocks'), [{ slot: 457320276, block_time: '2026-10-15 17:26:23', status: 'ok' }]);
  assert.equal(d.tables.get('events/meteora_dlmm/Swap2Evt')!.length, 1);
  assert.equal(d.tables.get('swaps/humidifi')!.length, 1);
  assert.ok(d.tables.get('intents')!.length >= 2);
  assert.ok(d.tables.get('decode_failures')!.some((f) => String(f.reason).startsWith('decode:')));
  assert.equal(d.stats.tx_total, 7);
  assert.equal(d.stats.tx_vote, 1);
  assert.equal(d.stats.tx_failed, 1);
  assert.equal(d.stats.tx_venue, 5);
  assert.equal(d.tables.get('events/meteora_dlmm/Swap2Evt')![0]!.tx_index, 2);
});

test('null block is a skipped slot', () => {
  const d = decodeBlock(null, 5, buildContext());
  assert.deepEqual(d.tables.get('blocks'), [{ slot: 5, block_time: null, status: 'skipped' }]);
  assert.equal(d.tables.size, 1);
});

test('a transaction with null meta is counted and skipped', () => {
  const b = blockOf(['humidifi_swap']);
  b.transactions[0] = { ...b.transactions[0]!, meta: null };
  const d = decodeBlock(b, 1, buildContext());
  assert.equal(d.stats.tx_null_meta, 1);
  assert.equal(d.tables.size, 1);
});
