# IDL sources

All files vendored on October 15–16, 2026. The decoder reads these files only; updating one is a code change that bumps `DECODER_VERSION`.

| File | Program | Source | Version |
| --- | --- | --- | --- |
| meteora_dlmm.json | LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo | github.com/MeteoraAg/dlmm-sdk `idls/dlmm.json` | 0.12.0 |
| meteora_damm_v2.json | cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG | github.com/MeteoraAg/zap-program `idls/damm_v2.json` | 0.1.5 |
| meteora_dbc.json | dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN | npm `@meteora-ag/dynamic-bonding-curve-sdk@2.0.0`, `idl_default` object in `dist/index.js` | 0.2.2 |
| orca_whirlpool.json | whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc | npm `@orca-so/whirlpools-sdk@0.22.0` `dist/artifacts/whirlpool.json` | 0.9.0 |
| raydium_clmm.json | CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK | github.com/raydium-io/raydium-idl `raydium_clmm/raydium_clmm.json` (master, Oct 15 2026). **Known mismatch:** deployed `SwapEvent` payload is 221 bytes and does not decode with this file; see spec §10 | 0.1.0 |
| raydium_cpmm.json | CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C | github.com/raydium-io/raydium-idl `raydium_cpmm/raydium_cp_swap.json` | 0.2.0 |
| raydium_launchlab.json | LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj | github.com/raydium-io/raydium-idl `raydium_launchpad/raydium_launchpad.json` | 0.2.0 |
| pumpswap.json | pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA | github.com/pump-fun/pump-public-docs `idl/pump_amm.json` | 0.1.0 |
| pumpfun.json | 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P | github.com/pump-fun/pump-public-docs `idl/pump.json` | 0.1.0 |
| (no file) raydium_amm_v4 | 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 | `ray_log` Borsh layouts transcribed from github.com/raydium-io/raydium-amm `program/src/log.rs` into `src/decoders/raydiumV4.ts` | master, Oct 16 2026 |
