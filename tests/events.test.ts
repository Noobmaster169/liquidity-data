import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeTier1 } from '../src/decoders/events.ts';
import { parseLogs } from '../src/logs.ts';
import { instructionAt } from '../src/tx.ts';
import { buildContext, fixtureTx } from './helpers.ts';

function run(name: string) { const tx = fixtureTx(name); return { tx, out: decodeTier1(tx, buildContext(), parseLogs(tx.logs)) }; }

test('DLMM Swap2Evt via self-CPI: envelope + verbatim fields', () => {
  const { out } = run('jupiter_multihop_dlmm_swap2');
  const rows = out.rows.get('events/meteora_dlmm/Swap2Evt')!;
  assert.equal(rows.length, 1);
  const r = rows[0]!;
  assert.equal(r.signature, '288dkjsD9qvbWaCArFypoRfjfB81a2kbd8qRwx1HJZWhwUVbt7w5LFLqnZvrtNSMs1kMHKp28dgGAs59Sun9dU3F');
  assert.equal(r.slot, 457320276);
  assert.equal(r.block_time, '2026-10-15 17:26:23');
  assert.equal(r.tx_index, 126);
  assert.equal(r.ix_path, '3.1');
  assert.equal(r.event_ordinal, 0);
  assert.equal(r.outer_program, 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(r.trader, '5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp');
  assert.equal(r.lb_pair, 'zxTpi4BtaWX3mgdAPoezkMD1hxx8CdeCfrqXMWvSCLX');
  assert.equal(r.amount_in, '104023597531');
  assert.equal(r.amount_out, '110065105');
  assert.equal(r.fee_bps, '2145563');
  assert.equal(r.end_bin_id, -3428);
  assert.equal(out.rows.get('events/meteora_dlmm/Swap')!.length, 1, 'the legacy Swap event is a second table');
  assert.equal(out.failures.length, 0);
});

test('events emitted on both channels produce one row', () => {
  const { out } = run('pumpswap_buy');
  assert.equal(out.rows.get('events/pumpswap/BuyEvent')!.length, 1);
  assert.equal(out.rows.get('events/pumpswap/BuyEvent')![0]!.ix_path, '6');
});

test('log-only events (Whirlpool Traded) decode with outer_program null on a direct call', () => {
  const { out } = run('whirlpool_traded');
  const r = out.rows.get('events/orca_whirlpool/Traded')![0]!;
  assert.equal(r.ix_path, '2');
  assert.equal(r.outer_program, null);
  assert.equal(typeof r.input_amount, 'string');
});

test('two different events from one instruction get ordinal 0 each; same event twice would get 0 and 1', () => {
  const { out } = run('dlmm_claim_fee');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee')![0]!.ix_path, '7');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee2')![0]!.ix_path, '7');
  assert.equal(out.rows.get('events/meteora_dlmm/ClaimFee2')![0]!.event_ordinal, 0);
  assert.ok(out.rows.get('events/meteora_dlmm/PositionClose'));
});

test('Raydium CLMM DecreaseLiquidityEvent gets pool_state from instruction account 3', () => {
  const { tx, out } = run('raydium_clmm_liquidity');
  const r = out.rows.get('events/raydium_clmm/DecreaseLiquidityEvent')![0]!;
  assert.equal(r.ix_path, '4');
  assert.equal(r.pool_state, instructionAt(tx, '4')!.accounts[3]);
});

test('Raydium CLMM SwapEvent with the mismatched IDL lands in failures, not in a table', () => {
  const { out } = run('raydium_clmm_swap');
  assert.equal(out.rows.get('events/raydium_clmm/SwapEvent'), undefined);
  const f = out.failures.find((x) => x.program === 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK')!;
  assert.match(String(f.reason), /^decode:/);
  assert.equal(typeof f.payload, 'string');
});

test('Raydium v4 ray_log becomes a row with amm', () => {
  const { out } = run('raydium_v4_raylog');
  const r = out.rows.get('events/raydium_amm_v4/SwapBaseIn')![0]!;
  assert.equal(r.amm, '3sjNoCnkkhWPVXYGDtem8rCciHSGc9jSFZuUAzKbvRVp');
  assert.equal(r.amount_in, '6707650079');
  assert.equal(r.trader, 'MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa');
});

test('log truncation: tier 1 instructions with no event get a log_truncated failure', () => {
  const { out } = run('log_truncated');
  assert.ok(out.rows.get('events/pumpswap/BuyEvent'));
  const reasons = out.failures.map((f) => f.reason);
  assert.ok(reasons.every((r) => r === 'log_truncated' || String(r).startsWith('decode:') || r === 'unknown_discriminator'));
});
