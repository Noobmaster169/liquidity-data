import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decodeTier2 } from '../src/decoders/tier2.ts';
import { buildContext, fixtureTx } from './helpers.ts';

test('HumidiFi hop under Jupiter becomes one swap row from the user side', () => {
  const out = decodeTier2(fixtureTx('humidifi_swap'), buildContext());
  const rows = out.rows.get('swaps/humidifi')!;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    signature: '15PAuXvXQeAYS3ZSmGiUwb6ubE6HkjUJRQWKk7TBBKvA3Nok19oThKdxP4rTuRwTczF6GUBr97TuG6BUcVCy5B7',
    slot: 457320309, block_time: '2026-10-15 17:26:30', tx_index: 349, ix_path: '4.5', event_ordinal: 0,
    outer_program: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', trader: '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd',
    program: '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp',
    mint_in: 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN', amount_in: '337601850',
    mint_out: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', amount_out: '105030197',
  });
  assert.equal(out.failures.length, 0);
});

test('Tessera V hop with ephemeral wSOL account', () => {
  const out = decodeTier2(fixtureTx('ephemeral_wsol_tier2_swap'), buildContext());
  const r = out.rows.get('swaps/tesserav')![0]!;
  assert.equal(r.mint_in, 'So11111111111111111111111111111111111111112');
  assert.equal(r.amount_in, '923667285');
  assert.equal(r.amount_out, '97899986');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
});

test('shared-accounts ZeroFi hop: user side is not the signer, trader is', () => {
  const out = decodeTier2(fixtureTx('jupiter_shared_accounts_hop'), buildContext());
  const r = out.rows.get('swaps/zerofi')![0]!;
  assert.equal(r.ix_path, '1.4');
  assert.equal(r.trader, 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe');
});

test('prop AMM instruction with no transfers produces nothing, counted in stats', () => {
  const out = decodeTier2(fixtureTx('propamm_oracle_update_no_transfers'), buildContext());
  assert.equal(out.rows.size, 0);
  assert.equal(out.failures.length, 0);
  assert.equal(out.stats.tier2_no_transfers, 1);
});

test('Manifest instruction without a swap-shaped party is skipped silently', () => {
  const out = decodeTier2(fixtureTx('manifest_non_swap'), buildContext());
  assert.equal(out.rows.size, 0);
  assert.equal(out.failures.length, 0);
});
