# Fixtures

Real mainnet transactions captured from finalized blocks 457320276–457320335 on October 16, 2026 with `getBlock(encoding: "json", transactionDetails: "full", maxSupportedTransactionVersion: 1)`. Each file is `{ name, slot, blockTime, txIndex, tx }` where `tx` is one element of the block's `transactions` array, unmodified.

| File | What it exercises |
| --- | --- |
| jupiter_multihop_dlmm_swap2 | Jupiter route; DLMM `swap2` at path 3.1 emitting `Swap` and `Swap2Evt` by self-CPI; signer is the trader |
| dlmm_liquidity | DLMM `PositionCreate` (path 1) then `AddLiquidity` + `CompositionFee` (path 5), direct call |
| dlmm_position_create | same transaction; `initialize_position` args lower_bin_id=-469, width=70 |
| dlmm_claim_fee | DLMM `RemoveLiquidity`, `ClaimFee` + `ClaimFee2` (same instruction, two events), `PositionClose` |
| damm_v2_swap2 | DAMM v2 `EvtSwap2` by self-CPI inside a DFlow route (same tx as dflow fixture) |
| dflow_route_with_fee_leg | DFlow router, 3 hops (DAMM v2, Tessera V), SOL fee leg to a pure recipient; intent = H8WTvo… in, USDC out |
| ephemeral_wsol_tier2_swap | same tx; Tessera V hop whose wSOL account is created by `InitializeAccount3` inside the tx |
| humidifi_swap | HumidiFi hop under Jupiter at path 4.5; trader = signer 4B8srv… |
| jupiter_shared_accounts_hop | ZeroFi hop at 1.4 where no hop party signs; user side by outside-use rule, trader by tx-level deltas |
| pumpswap_buy | PumpSwap `BuyEvent` emitted both by self-CPI and by log (must dedupe to one row) |
| pumpfun_trade | pump.fun `TradeEvent`, both channels |
| launchlab_trade | LaunchLab `TradeEvent`, both channels; also a Token-2022 `TransferChecked` |
| token2022_transfer_checked | same tx |
| whirlpool_traded | Whirlpool `Traded` by log only; version 1 transaction |
| version1_tx | same tx |
| whirlpool_liquidity | Whirlpool `LiquidityIncreased` by log |
| raydium_clmm_swap | CLMM `SwapEvent` by log, 221-byte payload that does not match the vendored IDL (expected `decode_failures`) |
| raydium_clmm_liquidity | CLMM `DecreaseLiquidityEvent` at path 4 (pool_state from instruction account 3) |
| raydium_cpmm_swap | CPMM `SwapEvent` by log at 2.8 and a DAMM v2 `EvtSwap2` at 2.3 in one route |
| raydium_v4_raylog | Raydium v4 `ray_log` SwapBaseIn (log_type 3, 57 bytes), amm = instruction account 1 |
| propamm_oracle_update_no_transfers | Tessera V top-level instruction with zero transfers: no row, no failure |
| manifest_non_swap | Manifest instruction with no swap-shaped party: skipped |
| log_truncated | transaction ending in `Log truncated` |
| failed_venue_tx | failed transaction touching Raydium v4: dropped |
| vote | vote transaction: dropped |
