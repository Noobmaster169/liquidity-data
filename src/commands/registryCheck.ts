import type { Registry } from '../registry.ts';
import type { RpcClient } from '../rpc.ts';

export async function registryCheck(registry: Registry, rpc: RpcClient): Promise<void> {
  let bad = 0;
  for (const [pid, v] of [...registry.tier1, ...registry.tier2]) {
    const exec = await rpc.getAccountExecutable(pid);
    const ok = exec === true;
    if (!ok) bad++;
    console.log(`${ok ? 'ok  ' : 'FAIL'} tier${v.tier} ${v.label.padEnd(28)} ${pid}${exec === null ? ' (no account)' : exec === false ? ' (not executable)' : ''}`);
  }
  console.log(bad === 0 ? 'all registry programs are executable on-chain' : `${bad} registry programs failed`);
  if (bad) process.exitCode = 1;
}
