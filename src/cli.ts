import { join } from 'node:path';
import { loadConfig, loadEnv } from './config.ts';
import { loadRegistry } from './registry.ts';
import { buildDecodeContext } from './decoders/context.ts';
import { RpcClient, RateLimiter } from './rpc.ts';
import { Checkpoint } from './checkpoint.ts';
import { RangeWriter } from './writer.ts';
import { openDuck } from './duck.ts';
import { schemaFor } from './tables.ts';
import { inlineDecoder, workerDecoder } from './decodePool.ts';
import { runLane } from './lane.ts';
import { DECODER_VERSION } from './version.ts';
import { status } from './commands/status.ts';
import { verify } from './commands/verify.ts';
import { redecode } from './commands/redecode.ts';
import { registryCheck } from './commands/registryCheck.ts';
import { query } from './commands/query.ts';

function args(argv: string[]): { positional: string[]; flags: Record<string, string> } {
  const positional: string[] = []; const flags: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a.startsWith('--')) { const k = a.slice(2); const v = argv[i + 1]; if (v !== undefined && !v.startsWith('--')) { flags[k] = v; i++; } else flags[k] = 'true'; }
    else positional.push(a);
  }
  return { positional, flags };
}

function resolveUntil(until: string, tipSlot: number): number {
  if (/^\d+$/.test(until)) return Number(until);
  const targetMs = Date.parse(until + 'T00:00:00Z');
  if (Number.isNaN(targetMs)) throw new Error(`--until must be a slot or YYYY-MM-DD, got ${until}`);
  const secondsBack = Math.max(0, (Date.now() - targetMs) / 1000);
  return Math.max(0, tipSlot - Math.round(secondsBack / 0.4)); // 400 ms slots; the lane stops at this slot
}

async function main(): Promise<void> {
  const { positional, flags } = args(process.argv.slice(2));
  const cmd = positional[0];
  const cfg = loadConfig();
  const registry = loadRegistry('config/venues.json', 'config/routers.json');
  const ctx = buildDecodeContext(registry);
  const log = (m: string) => console.log(`${new Date().toISOString()} ${m}`);

  if (cmd === 'tail' || cmd === 'backfill') {
    const { rpcUrl } = loadEnv();
    const laneCfg = cmd === 'tail' ? cfg.tail : cfg.backfill;
    const rpc = new RpcClient(rpcUrl, new RateLimiter(laneCfg.blocksPerSecond));
    await RangeWriter.cleanupTemp(cfg.dataRoot);
    const checkpoint = await Checkpoint.open(join(cfg.stateRoot, `${cmd}.duckdb`));
    const writer = new RangeWriter({ dataRoot: cfg.dataRoot, conn: await openDuck(':memory:'), decoderVersion: DECODER_VERSION, schemaFor: (t) => schemaFor(t, ctx), idlSha256: [...ctx.idlsByLabel.values()].map((i) => i.sha256.slice(0, 8)).join(',') });
    const decoder = cfg.workers > 0 ? workerDecoder(cfg.workers) : inlineDecoder(ctx);
    let fromSlot: number | undefined, targetSlot: number | undefined;
    if (cmd === 'backfill' && !(await checkpoint.getState('backfill'))) {
      const tailCkpt = await Checkpoint.open(join(cfg.stateRoot, 'tail.duckdb'));
      const tailState = await tailCkpt.getState('tail');
      await tailCkpt.close();
      if (!tailState) throw new Error('start the tail lane once before backfill so the start slot is recorded');
      fromSlot = tailState.startSlot - 1;
      if (!flags.until) throw new Error('backfill needs --until <YYYY-MM-DD|slot> on first start');
      targetSlot = resolveUntil(flags.until, tailState.startSlot);
      log(`backfill from ${fromSlot} down to ${targetSlot}`);
    }
    try {
      await runLane({ lane: cmd, rpc, checkpoint, writer, decoder, rangeSlots: cfg.rangeSlots, concurrency: laneCfg.concurrency, decoderVersion: DECODER_VERSION, tipMargin: cfg.tail.tipMargin, fromSlot, targetSlot, maxRanges: flags['max-ranges'] ? Number(flags['max-ranges']) : undefined, log });
    } finally { await decoder.close(); await checkpoint.close(); }
    return;
  }
  if (cmd === 'status') return status(cfg);
  if (cmd === 'verify') return verify(cfg, flags.lane ?? 'tail');
  if (cmd === 'redecode') return redecode(cfg, ctx, flags);
  if (cmd === 'registry' && positional[1] === 'check') return registryCheck(registry, new RpcClient(loadEnv().rpcUrl, new RateLimiter(5)));
  if (cmd === 'query') return query(cfg, flags.sql ?? positional.slice(1).join(' '));
  console.error('usage: fiber-data <tail|backfill --until D|status|verify [--lane L]|redecode|registry check|query --sql S> [--max-ranges N]');
  process.exit(2);
}

main().catch((e) => { console.error(e instanceof Error ? e.message : String(e)); process.exit(1); });
