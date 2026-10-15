import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RpcClient, RateLimiter, RpcError } from '../src/rpc.ts';

function fakeFetch(responses: Array<{ status?: number; body?: unknown; throws?: boolean }>) {
  const calls: unknown[] = [];
  const f = (async (_url: string | URL | Request, init?: RequestInit) => {
    calls.push(JSON.parse(String(init?.body)));
    const r = responses.shift() ?? { status: 200, body: { jsonrpc: '2.0', id: 1, result: null } };
    if (r.throws) throw new Error('socket hang up');
    return new Response(JSON.stringify(r.body ?? {}), { status: r.status ?? 200, headers: { 'content-type': 'application/json' } });
  }) as unknown as typeof fetch;
  return { f, calls };
}
const noSleep = async () => {};

test('getBlock sends the required parameters', async () => {
  const { f, calls } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, result: { blockTime: 1, blockHeight: 1, parentSlot: 9, transactions: [] } } }]);
  const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
  const r = await c.getBlock(10);
  assert.equal(r.kind, 'block');
  const req = calls[0] as { method: string; params: unknown[] };
  assert.equal(req.method, 'getBlock');
  assert.deepEqual(req.params, [10, { encoding: 'json', transactionDetails: 'full', maxSupportedTransactionVersion: 1, rewards: false, commitment: 'finalized' }]);
});

test('skipped-slot error codes return skipped', async () => {
  for (const code of [-32007, -32009]) {
    const { f } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, error: { code, message: 'Slot was skipped' } } }]);
    const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
    assert.deepEqual(await c.getBlock(10), { kind: 'skipped' });
  }
});

test('429 and -32004 are retried, then succeed; retries exhausted throw a retryable RpcError without the URL', async () => {
  const ok = { body: { jsonrpc: '2.0', id: 1, result: { blockTime: 1, blockHeight: 1, parentSlot: 9, transactions: [] } } };
  const { f } = fakeFetch([{ status: 429, body: {} }, { body: { jsonrpc: '2.0', id: 1, error: { code: -32004, message: 'Block not available for slot 10' } } }, ok]);
  const c = new RpcClient('https://rpc.example/secret-key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep, maxRetries: 5 });
  assert.equal((await c.getBlock(10)).kind, 'block');
  const bad = fakeFetch([{ status: 429 }, { status: 429 }, { status: 429 }]);
  const c2 = new RpcClient('https://rpc.example/secret-key', new RateLimiter(1000), { fetchImpl: bad.f, sleep: noSleep, maxRetries: 2 });
  await assert.rejects(() => c2.getBlock(10), (e: unknown) => e instanceof RpcError && e.retryable && !e.message.includes('secret-key'));
});

test('rate limiter spaces calls', async () => {
  const l = new RateLimiter(50);
  const t0 = Date.now();
  for (let i = 0; i < 6; i++) await l.take();
  assert.ok(Date.now() - t0 >= 90, 'six takes at 50/s need at least ~100ms');
});

test('a null getBlock result is not a skipped slot: it is retried and finally throws retryable', async () => {
  const ok = { body: { jsonrpc: '2.0', id: 1, result: { blockTime: 1, blockHeight: 1, parentSlot: 9, transactions: [] } } };
  const { f } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, result: null } }, ok]);
  const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
  assert.equal((await c.getBlock(10)).kind, 'block');
  const bad = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, result: null } }, { body: { jsonrpc: '2.0', id: 1, result: null } }]);
  const c2 = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: bad.f, sleep: noSleep, maxRetries: 1 });
  await assert.rejects(() => c2.getBlock(10), (e: unknown) => e instanceof RpcError && e.retryable);
});

test('a 429 halves the limiter rate and the rate recovers later', async () => {
  const l = new RateLimiter(100);
  const ok = { body: { jsonrpc: '2.0', id: 1, result: 5 } };
  const { f } = fakeFetch([{ status: 429 }, ok]);
  const c = new RpcClient('https://rpc.example/key', l, { fetchImpl: f, sleep: noSleep });
  await c.getSlot();
  assert.equal(l.currentRate(), 50);
  l.recover(Date.now() + 60_000);
  assert.equal(l.currentRate(), 100);
});

test('getBlockTime returns the block time or null', async () => {
  const { f } = fakeFetch([{ body: { jsonrpc: '2.0', id: 1, result: 1792085183 } }, { body: { jsonrpc: '2.0', id: 1, error: { code: -32009, message: 'skipped' } } }]);
  const c = new RpcClient('https://rpc.example/key', new RateLimiter(1000), { fetchImpl: f, sleep: noSleep });
  assert.equal(await c.getBlockTime(5), 1792085183);
  assert.equal(await c.getBlockTime(6), null);
});
