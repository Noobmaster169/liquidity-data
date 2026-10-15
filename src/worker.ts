import { parentPort } from 'node:worker_threads';
import { loadRegistry } from './registry.ts';
import { buildDecodeContext } from './decoders/context.ts';
import { decodeBlock } from './decoders/block.ts';
import type { RpcBlock } from './tx.ts';

const ctx = buildDecodeContext(loadRegistry('config/venues.json', 'config/routers.json'));
parentPort!.on('message', (m: { id: number; slot: number; block: RpcBlock | null }) => {
  try {
    const d = decodeBlock(m.block, m.slot, ctx);
    parentPort!.postMessage({ id: m.id, ok: true, slot: d.slot, tables: [...d.tables.entries()], stats: d.stats });
  } catch (e) {
    parentPort!.postMessage({ id: m.id, ok: false, error: e instanceof Error ? e.stack ?? e.message : String(e) });
  }
});
