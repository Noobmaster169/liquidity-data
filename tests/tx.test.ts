import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildTx, transfersUnder, instructionAt, WSOL_MINT } from '../src/tx.ts';
import { loadFixture, fixtureTx } from './helpers.ts';

const DLMM = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo';

test('instruction paths: outer 0-based, inner siblings from 1, nested depth from stackHeight', () => {
  const tx = fixtureTx('jupiter_multihop_dlmm_swap2');
  assert.equal(tx.instructions.length, 15);
  const dlmm = tx.instructions.filter((i) => i.programId === DLMM).map((i) => i.path);
  assert.deepEqual(dlmm, ['3.1', '3.1.3', '3.1.4']);
  assert.equal(tx.outerPrograms[3], 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
  assert.equal(instructionAt(tx, '3.1')?.accounts.length, 18);
});

test('signers, success, vote, version 1 and legacy', () => {
  const tx = fixtureTx('jupiter_multihop_dlmm_swap2');
  assert.deepEqual([...tx.signers], ['5op7ba3vhSGZgewfa4TGSMZU7wnHkVGmLFYunzzuQwxp', 'sighWH8KaiT7QhtV4w29ReVF8kG6D5yG3EQP1KYyGVF']);
  assert.equal(tx.success, true);
  assert.equal(fixtureTx('vote').isVote, true);
  assert.equal(fixtureTx('failed_venue_tx').success, false);
  assert.equal(fixtureTx('version1_tx').instructions.length, 5);
});

test('null meta yields null; null stackHeight is depth 2', () => {
  const f = loadFixture('whirlpool_traded');
  assert.equal(buildTx({ ...f.tx, meta: null }, f.slot, f.blockTime, f.txIndex), null);
  const copy = JSON.parse(JSON.stringify(f.tx));
  for (const g of copy.meta.innerInstructions ?? []) for (const ix of g.instructions) ix.stackHeight = null;
  const tx = buildTx(copy, f.slot, f.blockTime, f.txIndex)!;
  assert.ok(tx.instructions.every((i) => i.path.split('.').length <= 2));
});

test('token account owners come from balance metadata, InitializeAccount3, then transfer authority', () => {
  const tx = fixtureTx('ephemeral_wsol_tier2_swap');
  const init = instructionAt(tx, '0.1.4')!;
  assert.equal(init.data[0], 18);
  const info = tx.tokenAccounts.get(init.accounts[0]!)!;
  assert.equal(info.mint, WSOL_MINT);
  assert.equal(info.owner, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  const trs = transfersUnder(tx, '0.4');
  assert.equal(trs.length, 2);
  assert.ok(trs.every((t) => t.mint !== null && t.sourceOwner !== null && t.destOwner !== null));
});

test('Token-2022 TransferChecked carries its mint from the instruction accounts', () => {
  const tx = fixtureTx('token2022_transfer_checked');
  const t = tx.transfers.find((x) => x.path === '4.2')!;
  assert.equal(t.mint, '3CtbiqdLW3iHFqBd1vYZyENpVxjMMb5H8oxCRAvXnHsp');
  assert.equal(t.amount, 106740368042097n);
});

test('transaction-level token deltas are keyed owner|mint', () => {
  const tx = fixtureTx('jupiter_shared_accounts_hop');
  const signer = 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe';
  assert.ok([...tx.txDeltas.entries()].some(([k, v]) => k.startsWith(signer + '|') && v !== 0n));
});
