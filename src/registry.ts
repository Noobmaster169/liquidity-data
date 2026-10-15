import { readFileSync } from 'node:fs';

export interface VenueEntry { program_id: string; label: string; kind: string; tier: 0 | 1 | 2; idl?: string | null; reason?: string }

export interface Registry {
  tier1: Map<string, VenueEntry>;
  tier2: Map<string, VenueEntry>;
  routers: Map<string, string>;
  isVenue(programId: string): boolean;
  labelOf(programId: string): string;
}

export function loadRegistry(venuesPath: string, routersPath: string): Registry {
  const entries = JSON.parse(readFileSync(venuesPath, 'utf8')) as VenueEntry[];
  const routersRaw = JSON.parse(readFileSync(routersPath, 'utf8')) as Record<string, string>;
  const tier1 = new Map<string, VenueEntry>();
  const tier2 = new Map<string, VenueEntry>();
  for (const e of entries) {
    if (e.tier === 1) tier1.set(e.program_id, e);
    else if (e.tier === 2) tier2.set(e.program_id, e);
  }
  const routers = new Map(Object.entries(routersRaw));
  return {
    tier1, tier2, routers,
    isVenue: (p) => tier1.has(p) || tier2.has(p),
    labelOf: (p) => tier1.get(p)?.label ?? tier2.get(p)?.label ?? routers.get(p) ?? p,
  };
}
