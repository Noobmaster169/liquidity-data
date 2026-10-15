import type { Registry } from '../registry.ts';
import { loadIdl, type LoadedIdl } from '../idl.ts';

export interface DecodeContext { registry: Registry; idls: Map<string, LoadedIdl>; idlsByLabel: Map<string, LoadedIdl> }

export function buildDecodeContext(registry: Registry): DecodeContext {
  const idls = new Map<string, LoadedIdl>();
  const idlsByLabel = new Map<string, LoadedIdl>();
  for (const v of registry.tier1.values()) {
    if (!v.idl) continue;
    const idl = loadIdl(v.label, v.idl);
    idls.set(v.program_id, idl);
    idlsByLabel.set(v.label, idl);
  }
  return { registry, idls, idlsByLabel };
}
