import { test } from 'node:test';
import assert from 'node:assert/strict';
import { resolveUntil } from '../src/until.ts';
import type { RpcClient } from '../src/rpc.ts';

test('resolveUntil finds the first slot at or after the target date by block time, skipping null slots', async () => {
  const t0 = Date.parse('2026-10-01T00:00:00Z') / 1000;
  const rpc = { getBlockTime: async (slot: number) => (slot % 7 === 0 ? null : 1_000_000 + Math.floor((slot - 1_000_000) * 0.4)) } as unknown as RpcClient;
  const startSlot = 1_000_000 + Math.round((t0 + 86400 * 10 - 1_000_000) / 0.4); // ten days after the target
  const slot = await resolveUntil(rpc, '2026-10-01', startSlot);
  const bt = await rpc.getBlockTime(slot);
  assert.ok(bt !== null && bt >= t0 && bt - t0 < 5, `slot ${slot} block time ${bt} should be within 5 s after the target`);
  assert.equal(await resolveUntil(rpc, '123', startSlot), 123);
});
