import { test } from 'node:test';
import assert from 'node:assert/strict';
import { resolveTrader, netTransfers, swapParties } from '../src/trader.ts';
import { fixtureTx } from './helpers.ts';
import type { Transfer } from '../src/tx.ts';

test('direct signer is the trader; both parties net symmetrically', () => {
  const tx = fixtureTx('humidifi_swap');
  const r = resolveTrader(tx, '4.5');
  assert.equal(r.transfers.length, 2);
  assert.deepEqual([...r.parties].sort(), ['4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd', 'hKgG7iEDRFNsJSwLYqz8ETHuZwzh6qMMLow8VXa8pLm']);
  assert.equal(r.userSide, '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd');
  assert.equal(r.trader, '4B8srvotmWddTZuMt4RrBQrnx1sQj2Z7FaXgLbWvpVdd');
  const m = r.net.net.get(r.userSide!)!;
  assert.equal(m.get('JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'), -337601850n);
  assert.equal(m.get('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'), 105030197n);
});

test('shared-accounts hop: user side by outside-use rule, trader by transaction-level deltas', () => {
  const tx = fixtureTx('jupiter_shared_accounts_hop');
  const r = resolveTrader(tx, '1.4');
  assert.equal(r.userSide, 'ARu4n5mFdZogZAravu7CcizaojWnS6oqka37gdLT5SZn');
  assert.ok(!tx.signers.has(r.userSide!));
  assert.equal(r.trader, 'CpTescFMHdEzHPgjt7pTKxapBfEkd2qtuyY2rc6Nrioe');
});

test('ephemeral wSOL hop resolves through InitializeAccount3 ownership', () => {
  const r = resolveTrader(fixtureTx('ephemeral_wsol_tier2_swap'), '0.4');
  assert.equal(r.trader, '97itvmxTihn33ZJg3adr5hpgCmjdEU92oARk59yNYzv');
  assert.equal(r.net.net.get(r.userSide!)!.get('So11111111111111111111111111111111111111112'), -923667285n);
});

test('instruction without transfers: no parties, no trader', () => {
  const r = resolveTrader(fixtureTx('propamm_oracle_update_no_transfers'), '0');
  assert.equal(r.transfers.length, 0);
  assert.deepEqual(r.parties, []);
  assert.equal(r.userSide, null);
});

test('transfers with unknown mint are excluded from netting without throwing', () => {
  const trs: Transfer[] = [
    { path: '1.1', source: 'a', destination: 'b', mint: null, amount: 5n, sourceOwner: 'X', destOwner: 'Y' },
    { path: '1.2', source: 'c', destination: 'd', mint: 'M', amount: 7n, sourceOwner: 'Y', destOwner: 'X' },
  ];
  const n = netTransfers(trs);
  assert.deepEqual(swapParties(n), []);
  assert.equal(n.net.get('X')!.get('M'), 7n);
});
