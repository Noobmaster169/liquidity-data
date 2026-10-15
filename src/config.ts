import { existsSync, readFileSync } from 'node:fs';

export interface LaneConfig { blocksPerSecond: number; concurrency: number; tipMargin?: number }
export interface CollectorConfig { rangeSlots: number; dataRoot: string; stateRoot: string; workers: number; tail: LaneConfig & { tipMargin: number }; backfill: LaneConfig }

export function loadConfig(path = 'config/collector.json'): CollectorConfig {
  const c = JSON.parse(readFileSync(path, 'utf8')) as CollectorConfig;
  if (process.env.FIBER_DATA_ROOT) c.dataRoot = process.env.FIBER_DATA_ROOT;
  if (process.env.FIBER_STATE_ROOT) c.stateRoot = process.env.FIBER_STATE_ROOT;
  return c;
}

export function loadEnv(): { rpcUrl: string } {
  if (existsSync('.env')) {
    for (const line of readFileSync('.env', 'utf8').split('\n')) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/.exec(line);
      if (m && process.env[m[1]!] === undefined) process.env[m[1]!] = m[2]!.replace(/^["']|["']$/g, '');
    }
  }
  const rpcUrl = process.env.SOLANA_RPC_URL;
  if (!rpcUrl) throw new Error('SOLANA_RPC_URL is not set (put it in .env)');
  return { rpcUrl };
}
