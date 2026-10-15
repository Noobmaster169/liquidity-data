import type { RpcBlock } from './tx.ts';

export class RateLimiter {
  private perSecond: number;
  private tokens: number;
  private last = Date.now();
  constructor(perSecond: number) { this.perSecond = perSecond; this.tokens = 1; }
  setRate(perSecond: number): void { this.perSecond = Math.max(0.1, perSecond); }
  async take(): Promise<void> {
    for (;;) {
      const now = Date.now();
      this.tokens = Math.min(this.perSecond, this.tokens + ((now - this.last) / 1000) * this.perSecond);
      this.last = now;
      if (this.tokens >= 1) { this.tokens -= 1; return; }
      await new Promise((r) => setTimeout(r, Math.ceil(((1 - this.tokens) / this.perSecond) * 1000)));
    }
  }
}

export class RpcError extends Error {
  code: number | null; retryable: boolean;
  constructor(message: string, code: number | null, retryable: boolean) { super(message); this.code = code; this.retryable = retryable; }
}

export type BlockResult = { kind: 'block'; block: RpcBlock } | { kind: 'skipped' };

const SKIPPED = new Set([-32007, -32009]);
const RETRY_CODES = new Set([-32004, -32005, -32014, -32016]);
const sleepDefault = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class RpcClient {
  private url: string; private limiter: RateLimiter; private timeoutMs: number; private maxRetries: number;
  private fetchImpl: typeof fetch; private sleep: (ms: number) => Promise<void>;
  constructor(url: string, limiter: RateLimiter, opts: { timeoutMs?: number; maxRetries?: number; fetchImpl?: typeof fetch; sleep?: (ms: number) => Promise<void> } = {}) {
    this.url = url; this.limiter = limiter; this.timeoutMs = opts.timeoutMs ?? 60_000; this.maxRetries = opts.maxRetries ?? 8;
    this.fetchImpl = opts.fetchImpl ?? fetch; this.sleep = opts.sleep ?? sleepDefault;
  }

  private async call(method: string, params: unknown[]): Promise<{ result?: unknown; error?: { code: number; message: string } }> {
    let attempt = 0;
    for (;;) {
      await this.limiter.take();
      try {
        const res = await this.fetchImpl(this.url, {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(this.timeoutMs),
        });
        if (res.status === 429 || res.status >= 500) throw new RpcError(`${method}: HTTP ${res.status}`, null, true);
        if (!res.ok) throw new RpcError(`${method}: HTTP ${res.status}`, null, false);
        const json = (await res.json()) as { result?: unknown; error?: { code: number; message: string } };
        if (json.error && RETRY_CODES.has(json.error.code)) throw new RpcError(`${method}: ${json.error.code} ${json.error.message}`, json.error.code, true);
        return json;
      } catch (e) {
        const err = e instanceof RpcError ? e : new RpcError(`${method}: ${e instanceof Error ? e.message : String(e)}`, null, true);
        if (!err.retryable || attempt >= this.maxRetries) throw err;
        attempt++;
        await this.sleep(Math.min(30_000, 250 * 2 ** attempt));
      }
    }
  }

  async getSlot(): Promise<number> {
    const r = await this.call('getSlot', [{ commitment: 'finalized' }]);
    if (r.error) throw new RpcError(`getSlot: ${r.error.code} ${r.error.message}`, r.error.code, false);
    return Number(r.result);
  }

  async getBlock(slot: number): Promise<BlockResult> {
    const r = await this.call('getBlock', [slot, { encoding: 'json', transactionDetails: 'full', maxSupportedTransactionVersion: 1, rewards: false, commitment: 'finalized' }]);
    if (r.error) {
      if (SKIPPED.has(r.error.code)) return { kind: 'skipped' };
      throw new RpcError(`getBlock(${slot}): ${r.error.code} ${r.error.message}`, r.error.code, false);
    }
    if (r.result === null || r.result === undefined) return { kind: 'skipped' };
    return { kind: 'block', block: r.result as RpcBlock };
  }

  async getAccountExecutable(pubkey: string): Promise<boolean | null> {
    const r = await this.call('getAccountInfo', [pubkey, { encoding: 'base64', commitment: 'finalized' }]);
    const v = (r.result as { value?: { executable?: boolean } | null } | undefined)?.value;
    return v ? Boolean(v.executable) : null;
  }
}
