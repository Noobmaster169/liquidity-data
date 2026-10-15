// Generates config/venues.json from the Jupiter program-id-to-label snapshot plus curated tier 1 and exclusion lists.
import { readFileSync, writeFileSync } from 'node:fs';

const source = JSON.parse(readFileSync('config/sources/jupiter_program_id_to_label_2026-10-15.json', 'utf8')) as Record<string, string>;
const routers = JSON.parse(readFileSync('config/routers.json', 'utf8')) as Record<string, string>;

const TIER1: Record<string, { label: string; kind: string; idl: string | null }> = {
  LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo: { label: 'meteora_dlmm', kind: 'dlmm', idl: 'idl/meteora_dlmm.json' },
  cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG: { label: 'meteora_damm_v2', kind: 'clmm', idl: 'idl/meteora_damm_v2.json' },
  dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN: { label: 'meteora_dbc', kind: 'bonding_curve', idl: 'idl/meteora_dbc.json' },
  whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc: { label: 'orca_whirlpool', kind: 'clmm', idl: 'idl/orca_whirlpool.json' },
  CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK: { label: 'raydium_clmm', kind: 'clmm', idl: 'idl/raydium_clmm.json' },
  CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C: { label: 'raydium_cpmm', kind: 'amm', idl: 'idl/raydium_cpmm.json' },
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': { label: 'raydium_amm_v4', kind: 'amm', idl: null },
  LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj: { label: 'raydium_launchlab', kind: 'bonding_curve', idl: 'idl/raydium_launchlab.json' },
  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: { label: 'pumpswap', kind: 'amm', idl: 'idl/pumpswap.json' },
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': { label: 'pumpfun', kind: 'bonding_curve', idl: 'idl/pumpfun.json' },
};
// Programs in the Jupiter list that are not spot swaps. Kept with tier 0 and a reason so the exclusion is visible.
const EXCLUDE: Record<string, string> = {
  'Sanctum': 'LST stake/unstake router', 'Sanctum Infinity': 'LST pool', 'Sanctum Prop S': 'LST', 'Sanctum SOLS': 'LST',
  'Jupiter Lend Earn': 'lending', 'JupiterRfqV2': 'router, see routers.json', 'Perps': 'perpetuals', 'Helium Network': 'not a DEX',
  'Solayer': 'restaking', 'XOrca': 'staking', 'VaultLiquidUnstake': 'unstake', 'Carrot': 'yield vault', 'Voltr': 'vault',
  'Huma': 'credit', 'Perena': 'stable LP mint', 'Perena Star V2': 'stable LP mint',
};
const EXTRA: { program_id: string; label: string; kind: string }[] = [
  { program_id: 'goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j', label: 'goonfi', kind: 'prop' },
  { program_id: '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c', label: 'lifinity_v2', kind: 'prop' },
  { program_id: 'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S', label: 'lifinity_v1', kind: 'prop' },
  { program_id: '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', label: 'orca_v2', kind: 'amm' },
];
const PROP = new Set(['BisonFi', 'TesseraV', 'HumidiFi', 'GoonFi V2', 'SolFi', 'SolFi V2', 'ZeroFi', 'Obric V2', 'Quantum', 'AlphaQ', 'Scorch', 'Hadron', 'Obsidian', 'Archer']);
const CLOB = new Set(['Manifest', 'Phoenix', 'OpenBook V2']);

function slug(s: string): string { return s.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, ''); }

const out: Record<string, unknown>[] = [];
for (const [pid, t] of Object.entries(TIER1)) out.push({ program_id: pid, label: t.label, kind: t.kind, tier: 1, idl: t.idl });
for (const [pid, name] of Object.entries(source)) {
  if (TIER1[pid] || routers[pid]) continue;
  if (EXCLUDE[name]) { out.push({ program_id: pid, label: slug(name), kind: 'excluded', tier: 0, reason: EXCLUDE[name] }); continue; }
  out.push({ program_id: pid, label: slug(name), kind: PROP.has(name) ? 'prop' : CLOB.has(name) ? 'clob' : 'amm', tier: 2 });
}
for (const e of EXTRA) if (!out.some((o) => o.program_id === e.program_id)) out.push({ ...e, tier: 2 });
writeFileSync('config/venues.json', JSON.stringify(out, null, 2) + '\n');
console.log(`wrote ${out.length} entries: tier1=${out.filter((o) => o.tier === 1).length} tier2=${out.filter((o) => o.tier === 2).length} excluded=${out.filter((o) => o.tier === 0).length}`);
