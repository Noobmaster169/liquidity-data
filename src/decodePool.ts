import { Worker } from 'node:worker_threads';
import type { RpcBlock } from './tx.ts';
import { decodeBlock, type DecodedBlock } from './decoders/block.ts';
import type { DecodeContext } from './decoders/context.ts';

export interface Decoder { decode(block: RpcBlock | null, slot: number): Promise<DecodedBlock>; close(): Promise<void> }

export function inlineDecoder(ctx: DecodeContext): Decoder {
  return { decode: async (block, slot) => decodeBlock(block, slot, ctx), close: async () => {} };
}

interface Pending { resolve: (d: DecodedBlock) => void; reject: (e: Error) => void }

export function workerDecoder(n: number): Decoder {
  const workers = Array.from({ length: n }, () => new Worker(new URL('./worker.ts', import.meta.url)));
  const pending = new Map<number, Pending>();
  let next = 0, rr = 0;
  for (const w of workers) {
    w.on('message', (m: { id: number; ok: boolean; tables?: [string, Record<string, unknown>[]][]; stats?: Record<string, number>; slot?: number; error?: string }) => {
      const p = pending.get(m.id); if (!p) return; pending.delete(m.id);
      if (m.ok) p.resolve({ slot: m.slot!, tables: new Map(m.tables), stats: m.stats ?? {} }); else p.reject(new Error(m.error));
    });
    w.on('error', (e) => { for (const p of pending.values()) p.reject(e); pending.clear(); });
  }
  return {
    decode: (block, slot) => new Promise((resolve, reject) => {
      const id = next++; pending.set(id, { resolve, reject });
      workers[rr++ % workers.length]!.postMessage({ id, slot, block });
    }),
    close: async () => { await Promise.all(workers.map((w) => w.terminate())); },
  };
}
