import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeIntents } from '../src/decoders/intents.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('DFlow route: fee leg removed, intent amounts are the trader gross, hops counted', () => {
  const out = decodeIntents(fixtureTx('dflow_route_with_fee_leg'), buildContext());
  const rows = out.rows.get('intents')!;
  assert.equal(rows.length, 1);
  const r = rows[0]!;
  assert.equal(r.ix_path, '0');
  assert.equal(r.outer_program, null);
  assert.equal(r.program, 'DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  assert.equal(r.kind, 'swap');
  assert.equal(r.mint_in, 'H8WTvo3ZXSYHJAcYqzCforbG8coR1FDAX2kHFfqHBAGS');
  assert.equal(r.amount_in, '84566873727156');
  assert.equal(r.mint_out, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
  assert.equal(r.amount_out, '97899986');
  assert.equal(r.hops, 3);
});

test('direct DLMM swap under Jupiter: one intent for the route', () => {
  const out = decodeIntents(fixtureTx('jupiter_multihop_dlmm_swap2'), buildContext());
  const r = out.rows.get('intents')![0]!;
  assert.equal(r.ix_path, '3');
  assert.equal(r.program, 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(r.trader, '5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp');
  assert.ok(r.kind === 'swap' || r.kind === 'cycle');
});

test('outer instructions with no transfers or no swap-shaped party are skipped silently', () => {
  const a = decodeIntents(fixtureTx('propamm_oracle_update_no_transfers'), buildContext());
  assert.equal(a.rows.size, 0); assert.equal(a.failures.length, 0); assert.equal(a.stats.intent_no_transfers, 1);
  const b = decodeIntents(fixtureTx('manifest_non_swap'), buildContext());
  assert.equal(b.rows.size, 0); assert.equal(b.failures.length, 0);
});

test('arbitrage cycle: the mint that nets non-zero is the cycle mint and amounts are its gross flows', () => {
  // humidifi_swap is USDC -> USDT -> JUP -> USDC in one Jupiter route; the trader ends with more USDC
  const out = decodeIntents(fixtureTx('humidifi_swap'), buildContext());
  const r = out.rows.get('intents')![0]!;
  assert.equal(r.kind, 'cycle');
  assert.equal(r.mint_in, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
  assert.equal(r.mint_out, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
  assert.equal(r.hops, 3);
  assert.ok(BigInt(String(r.amount_out)) > BigInt(String(r.amount_in)), 'profit is amount_out minus amount_in');
});

test('relayer-paid transaction: the fee payer is not the trader', () => {
  const tx = fixtureTx('dflow_route_with_fee_leg');
  const feePayer = tx.keys[0]!;
  const r = decodeIntents(tx, buildContext()).rows.get('intents')![0]!;
  assert.notEqual(r.trader, feePayer);
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
});
