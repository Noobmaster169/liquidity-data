# fiber-data: Solana DEX raw event collector — design

Date: October 16, 2026 (revised). Status: draft for review.

## 1. Purpose

fiber-data collects on-chain swap and liquidity activity from Solana DEX programs and
stores it as queryable tables for quantitative analysis: backtesting, liquidity and
volume dynamics, LP strategy evaluation.

It stores what programs emitted, field for field from their IDLs. The one exception is
proprietary AMMs, which emit nothing; for them it stores one swap row per venue call,
derived from the token transfers inside the call by a rule stated in §4.

The analyses this dataset must support, chosen by the user:

1. Volume and flow per venue, pool and pair.
2. LP position lifecycle and fee earnings.
3. Router and frontend attribution of swaps.

### Decisions already made

| Topic | Decision |
| --- | --- |
| Venue scope | Near-all Solana spot swap venues, not Jupiter-only |
| History | Live tail plus about 90 days of backfill |
| Source | RPC `getBlock` over every finalized slot, filtered by a venue registry |
| Raw transactions | Not stored (measured at 3–6 TB per 90 days) |
| Storage | Parquet files partitioned by UTC day, queried with DuckDB |
| Tier 1 data model | One table per (program, event type), columns copied from the IDL |
| Tier 2 data model | One swap row per venue call, netted from token transfers |
| Failed transactions | Dropped; nothing executed |
| Language | TypeScript on Node 24, IDL-driven decoding |

## 2. Architecture

```mermaid
flowchart LR
    RPC[("Solana RPC getBlock")]
    subgraph collector["fiber-data (one binary)"]
        direction TB
        TAIL["tail lane: follows finalized slots"]
        BACK["backfill lane: walks backward 90 days"]
        FETCH["block fetcher, rate limited"]
        FILTER["venue filter, registry lookup"]
        DEC["decoders, worker threads"]
        WRITE["range writer, Parquet per table"]
        CKPT[("checkpoint state/*.duckdb")]
    end
    subgraph data["data/ (Parquet)"]
        EV["events/&lt;program&gt;/&lt;Event&gt;/date=.../"]
        SW["swaps/&lt;program&gt;/date=.../"]
        INS["instructions/meteora_dlmm/initialize_position/date=.../"]
        COV["blocks/, decode_failures/"]
    end
    Q["analysis: DuckDB SQL, pandas, Polars"]

    TAIL --> FETCH
    BACK --> FETCH
    RPC --> FETCH --> FILTER --> DEC --> WRITE
    WRITE --> EV & SW & INS & COV
    WRITE --> CKPT
    EV & SW & INS & COV --> Q
```

### Components

| Component | Responsibility | Depends on |
| --- | --- | --- |
| `tail` lane | From the finalized slot at first start, follow new finalized slots forward in ranges | fetcher, writer, its own checkpoint |
| `backfill` lane | From the slot `tail` started at, walk backward in ranges until the target slot (about 90 days) | fetcher, writer, its own checkpoint |
| block fetcher | `getBlock` with `encoding: "json"`, `transactionDetails: "full"`, `maxSupportedTransactionVersion: 1`, `rewards: false`; token-bucket rate limit; retry with backoff | RPC URL from environment |
| venue filter | Drop vote and failed transactions. Keep a transaction if any outer or inner instruction's program is in the registry | `config/venues.json` |
| decoders | Tier 1: Anchor event decoding from the vendored IDLs; Raydium AMM v4 `ray_log` decoder. Tier 2: swap netting from token transfers | IDL files in `idl/` |
| range writer | Buffer rows per table for one slot range, write Parquet, sync, rename, commit checkpoint | DuckDB (`@duckdb/node-api`) |
| `verify` | Check slot coverage and file checksums over a range | checkpoint, data |
| `redecode` | Re-run decoders on `decode_failures` rows without RPC | decoders, data |

The JSON encoding makes the RPC node parse legacy, v0 and v1 messages, so the collector
has no wire-format parser. A transaction's full account list is `message.accountKeys`
followed by `meta.loadedAddresses.writable` then `.readonly`.

## 3. Venue registry

`config/venues.json` is the single list of programs the collector cares about, seeded
from Jupiter's `program-id-to-label` endpoint (107 programs on October 15, 2026) plus
Vybe's DEX list for programs Jupiter omits. Each entry:

```json
{ "program_id": "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
  "label": "meteora_dlmm", "kind": "dlmm", "tier": 1, "idl": "idl/meteora_dlmm.json" }
```

Inclusion rule: the program executes an atomic spot exchange of two tokens in one
instruction. Programs present in the source lists but excluded (LST stake routers such as
Sanctum and Solayer, lending, perps, Helium) stay in the file with `"kind": "excluded"`
and a reason, so exclusions are visible.

### Tier 1: event decoders (10 programs)

| Label | Program | IDL source | Events | Swap event | Liquidity, position and claim events |
| --- | --- | --- | --- | --- | --- |
| meteora_dlmm | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | dlmm-sdk `idls/dlmm.json` v0.12.0 | 30 | Swap, Swap2Evt | AddLiquidity, RemoveLiquidity, Rebalancing, PositionCreate, PositionClose, ClaimFee, ClaimFee2, ClaimReward, ClaimReward2 |
| meteora_damm_v2 | `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG` | `idls/damm_v2.json` v0.1.5 | 27 | EvtSwap, EvtSwap2 | EvtAddLiquidity, EvtRemoveLiquidity, EvtLiquidityChange, EvtCreatePosition, EvtClosePosition, EvtClaimPositionFee, EvtClaimReward |
| meteora_dbc | `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` | `@meteora-ag/dynamic-bonding-curve-sdk` 2.0.0 bundled IDL | 21 | EvtSwap | none (bonding curve) |
| orca_whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | `@orca-so/whirlpools-sdk` 0.22.0 `whirlpool.json` | 6 | Traded | LiquidityIncreased, LiquidityDecreased, LiquidityRepositioned, PositionOpened |
| raydium_clmm | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | raydium-idl `raydium_clmm.json` | 15 | SwapEvent | IncreaseLiquidityEvent, DecreaseLiquidityEvent, CreatePersonalPositionEvent, CollectPersonalFeeEvent, LiquidityChangeEvent |
| raydium_cpmm | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | raydium-idl `raydium_cp_swap.json` v0.2.0 | 2 | SwapEvent | LpChangeEvent |
| raydium_amm_v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | no Anchor IDL; `ray_log` Borsh layout from raydium-amm source | 5 log variants | SwapBaseIn, SwapBaseOut | Deposit, Withdraw, Init |
| raydium_launchlab | `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj` | raydium-idl `raydium_launchpad.json` v0.2.0 | 4 | TradeEvent | none (bonding curve) |
| pumpswap | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | pump-public-docs `pump_amm.json` | 27 | BuyEvent, SellEvent | DepositEvent, WithdrawEvent, CreatePoolEvent |
| pumpfun | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | pump-public-docs `pump.json` | 32 | TradeEvent | none (bonding curve) |

Every event the IDL defines gets a table, administrative ones included; they are rare
and filtering is not the collector's job.

### Tier 2: swap rows only

All other registry venues. Notably the proprietary AMMs, which carry roughly a third of
Solana spot volume and emit no decodable events:

| Label | Program |
| --- | --- |
| bisonfi | `BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi` |
| tessera_v | `TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH` |
| humidifi | `9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp` |
| goonfi, goonfi_v2 | `goonERTdGsjnkZqWuVjs73BZ3Pb9qoCUdBUL17BnS5j`, `goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE` |
| solfi, solfi_v2 | `SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe`, `SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF` |
| zerofi | `ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY` |
| obric_v2 | `obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y` |
| quantum | `QuaNtZsgYRe5Z9Bk4LZ4cTD9tbkVoyCNf1R2BN9bBDv` |
| alphaq | `ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA` |
| scorch | `ojh19ojaKduoJZuaJADhcVGp4xt1TcdAvZmpVsCorch` |
| manifest, phoenix, openbook_v2 (CLOBs; fills only) | `MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms`, `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY`, `opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb` |
| long tail | Lifinity v1/v2, Meteora DAMM v1, Saber, Stabble, Invariant, Byreal, Aquifer, PancakeSwap, Saros, Crema, Bonkswap, GooseFX, FluxBeam, Woofi, and the rest of the Jupiter list |

### Routers

`config/routers.json`: Jupiter v6 `JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4`, Jupiter RFQ
`61DFfeTKM7trxYcPQCM78bJ794ddZprZpAwAnLiwTpYH` and `fd3nMFYTQjX1yr5ER8u7tPdHJB7qt8RpDpNtLQX2Br5`,
Raydium Routing `routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS`, Titan
`T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT`, plus OKX DEX Router and DFlow once verified
on-chain. Routers never produce rows of their own; they appear as `outer_program` on the
venue rows they wrap. The registry is used for labelling only, not for capture: any
program at the top of a venue call is recorded, known router or not.

## 4. Data model

### Principles

1. A tier 1 table's columns are the IDL event's fields, with the IDL's names and types.
   Nothing is renamed, merged, dropped or computed.
2. A fixed seven-column envelope says where the row came from. It is identical on every
   table.
3. A column exists only if one of the three analyses in §1 needs it.
4. Amounts are integers in base units. No floats anywhere.

### Envelope (every table)

| Column | Type | Needed for |
| --- | --- | --- |
| `signature` | VARCHAR | identity, lookup |
| `slot` | UBIGINT | ordering, partition and range key |
| `block_time` | TIMESTAMP (UTC, seconds) | time axis. Derivable from `blocks` by slot, kept on the row so billion-row scans need no join |
| `tx_index` | UINTEGER | order within a block, so two swaps on one pool in the same slot can be sequenced |
| `ix_path` | VARCHAR, e.g. `4.7` | which instruction produced the row; one transaction with four hops gives four rows |
| `event_ordinal` | USMALLINT | order among events of the same type from one instruction |
| `outer_program` | VARCHAR, null | the program of the top-level instruction when it differs from the venue: router or frontend attribution (analysis 3). Null for direct calls |

Row identity is `(signature, ix_path, event_ordinal)`.

### Tier 1: `events/<program>/<Event>`

Envelope plus the IDL fields. Three event types do not name their pool, and for exactly
those the decoder adds one column taken from the IDL-named account of the emitting
instruction:

| Table | Added column | Source |
| --- | --- | --- |
| `raydium_amm_v4/*` (all five ray_log variants) | `amm` | the instruction's `amm` account |
| `raydium_clmm/IncreaseLiquidityEvent` | `pool_state` | `increase_liquidity*` account index 2 |
| `raydium_clmm/DecreaseLiquidityEvent` | `pool_state` | `decrease_liquidity*` account index 3 |

Type mapping (Borsh to Parquet, mechanical):

| IDL type | Column type |
| --- | --- |
| `u8`, `u16`, `u32` | UINTEGER |
| `u64` | UBIGINT |
| `i8`, `i16`, `i32` | INTEGER |
| `i64` | BIGINT |
| `u128`, `i128` | HUGEINT |
| `bool` | BOOLEAN |
| `string` | VARCHAR |
| `pubkey` | VARCHAR (base58) |
| `[T; N]`, `vec<T>` | LIST |
| `defined` struct | STRUCT with the struct's field names, nested |
| enum | VARCHAR, variant name |
| `option<T>` | nullable |

Example, a real row from slot 457304333 in `events/meteora_dlmm/Swap2Evt`:

```text
signature        4ZhGP3TBXtCQcKc9bVrhdLEbGzw3zbtTZFaejt7zenda912KDyWt33EgNmdF42HNYQwLwndRV8ga6Bg5osipEprz
slot             457304333
block_time       2026-10-15 16:28:49
tx_index         683
ix_path          4.7
event_ordinal    0
outer_program    JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4
--- IDL fields ---
lb_pair          G7ixPyiyNeggVf1VanSetFMNbVuVCPtimJmd9axfQqng
from             NALUxTJsozrScVNzRWy9LAg2xoC37RXNNZyyem76kmc
start_bin_id     37681
end_bin_id       37681
swap_for_y       true
fee_bps          134311
amount_in        13311167
amount_left      0
amount_out       576157224
mm_fee           1610
protocol_fee     178
limit_order_fee  0
host_fee         0
fees_on_input    true
fees_on_token_x  true
```

### Tier 2: `swaps/<program>`

One row per tier 2 venue call that is a swap. Envelope (with `event_ordinal` always 0)
plus:

| Column | Type | Needed for |
| --- | --- | --- |
| `program` | VARCHAR | the venue (analysis 1) |
| `trader` | VARCHAR | flow per wallet (analysis 1) |
| `mint_in`, `amount_in` | VARCHAR, UBIGINT | what the trader gave (analysis 1) |
| `mint_out`, `amount_out` | VARCHAR, UBIGINT | what the trader received (analysis 1) |

**Netting rule.** Inside the venue call's CPI subtree, take every SPL Token and Token-2022
`Transfer` and `TransferChecked`, and every System `Transfer` (native SOL, mint
`So11…1112`). Resolve each token account's owner and mint from the block's token balance
metadata. Sum amounts per (owner, mint) as sent or received. The owner who sent exactly
one mint and received exactly one different mint is the trader; those two sums are
`amount_in` and `amount_out`. If no owner fits, or more than one does, the call is not
recorded as a swap and goes to `decode_failures` with reason `not_a_swap` and the raw
transfers attached. This is a derivation, the only one in the collector, and it exists
because proprietary AMMs emit nothing else.

Example, a real row from slot 457304346 in `swaps/humidifi`, routed by Titan:

```text
signature        aZ3u52RVsFPxBCaf7JHJxsZixXmioRWQjepDcBDSVSBcFM98wcLdtXTGdiVQXKaBZ1ntj8vbL6hoovfcf4cpH6x
slot             457304346
block_time       2026-10-15 16:28:54
tx_index         <from block>
ix_path          0.13
event_ordinal    0
outer_program    T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT
program          9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp
trader           HyKbc1…
mint_in          pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn
amount_in        942027658
mint_out         EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
amount_out       4238790
```

### Instruction table: `instructions/meteora_dlmm/initialize_position`

A DLMM position's bin range is passed as instruction arguments and appears in no event.
Analysis 2 cannot tell whether a position was in range without it. This is the only
instruction table. Rows come from `initialize_position`, `initialize_position2`,
`initialize_position_pda` and `initialize_position_by_operator`. Columns: envelope, the
IDL-named accounts `position` and `lb_pair`, and the IDL arguments `lower_bin_id` (INTEGER)
and `width` (INTEGER). The variant's extra arguments (`fee_owner`, `lock_release_point`)
are included for `initialize_position_by_operator` and null otherwise.

### Coverage and failures

| Table | Columns | Needed for |
| --- | --- | --- |
| `blocks` | `slot`, `block_time`, `status` (`ok` or `skipped`) | proof that no slot was missed; `verify` reads it |
| `decode_failures` | `signature`, `slot`, `ix_path`, `program`, `reason`, `ix_data` (BLOB), `log_lines` (LIST), `transfers` (LIST of STRUCT source, destination, owner_source, owner_destination, mint, amount) | the only place instruction bytes, log lines and raw transfers are kept; input to `redecode` |

### Entity diagram

```mermaid
erDiagram
    BLOCKS ||--o{ EVENT : slot
    BLOCKS ||--o{ SWAP : slot
    BLOCKS ||--o{ DLMM_INITIALIZE_POSITION : slot
    EVENT ||--o| DECODE_FAILURE : "same signature and ix_path"
    SWAP ||--o| DECODE_FAILURE : "same signature and ix_path"
    DLMM_INITIALIZE_POSITION ||--o{ EVENT : "position, lb_pair"

    EVENT {
        varchar signature PK
        ubigint slot
        timestamp block_time
        uinteger tx_index
        varchar ix_path PK
        usmallint event_ordinal PK
        varchar outer_program "null when direct"
        any idl_field_1 "verbatim from the IDL"
        any idl_field_n "verbatim from the IDL"
        varchar amm_or_pool_state "only on the 3 events without a pool field"
    }
    SWAP {
        varchar signature PK
        ubigint slot
        timestamp block_time
        uinteger tx_index
        varchar ix_path PK
        usmallint event_ordinal PK "always 0"
        varchar outer_program "null when direct"
        varchar program "the tier 2 venue"
        varchar trader
        varchar mint_in
        ubigint amount_in
        varchar mint_out
        ubigint amount_out
    }
    DLMM_INITIALIZE_POSITION {
        varchar signature PK
        ubigint slot
        timestamp block_time
        uinteger tx_index
        varchar ix_path PK
        usmallint event_ordinal PK "always 0"
        varchar outer_program
        varchar position
        varchar lb_pair
        integer lower_bin_id
        integer width
        varchar fee_owner "by_operator only"
        ubigint lock_release_point "by_operator only"
    }
    BLOCKS {
        ubigint slot PK
        timestamp block_time
        varchar status "ok or skipped"
    }
    DECODE_FAILURE {
        varchar signature PK
        ubigint slot
        varchar ix_path PK
        varchar program
        varchar reason
        blob ix_data
        list log_lines
        list transfers "raw transfers for not_a_swap"
    }
```

`EVENT` stands for every tier 1 table, e.g. `events/meteora_dlmm/Swap2Evt` or
`events/orca_whirlpool/Traded`; each has its own IDL columns.

### What the analyses read

| Analysis | Reads | Resolved later, outside the collector |
| --- | --- | --- |
| 1. Volume per venue, pool, pair | tier 1 swap events (pool, amounts), tier 2 `swaps` (mints, amounts) | pool to mints for tier 1 pools, by a one-time RPC lookup of each pool account seen; pool accounts persist on chain. Token decimals, by `getMint` for each mint seen |
| 2. LP lifecycle and fees | tier 1 liquidity, position and claim events; `initialize_position` for DLMM ranges; DLMM `end_bin_id` on swaps gives the active-bin path | none |
| 3. Router and frontend attribution | `outer_program` | labels for program IDs, from `config/routers.json`. Frontends that charge by a plain SOL transfer and use no program of their own are not identified |

### Not stored, deliberately

Raw transactions. Events of failed transactions. Instruction bytes and log lines of
successful instructions. Account lists. Token transfers of tier 1 venues (their events
carry the amounts). Pool state beyond what events carry. Token decimals, names and
symbols. USD values. Order-book placements on CLOBs.

### Layout on disk

```text
data/
  events/meteora_dlmm/Swap2Evt/date=2026-10-15/457300000-457305000.parquet
  events/pumpswap/BuyEvent/date=2026-10-15/...
  swaps/humidifi/date=2026-10-15/...
  instructions/meteora_dlmm/initialize_position/date=2026-10-15/...
  blocks/date=2026-10-15/...
  decode_failures/date=2026-10-15/...
state/tail.duckdb
state/backfill.duckdb
```

Parquet files are ZSTD compressed, sorted by `(slot, tx_index, ix_path)`, with key-value
metadata `decoder_version`, `idl_sha256`, `lane`, `lo_slot`, `hi_slot`. Query example:
`SELECT * FROM read_parquet('data/events/meteora_dlmm/Swap2Evt/**/*.parquet')`.

## 5. Collection mechanics

### Lanes and ranges

```mermaid
sequenceDiagram
    participant L as lane (tail or backfill)
    participant F as fetcher
    participant R as RPC
    participant D as decoder pool
    participant W as range writer
    participant C as checkpoint

    L->>C: next range [lo, hi] (5,000 slots)
    loop each slot, N concurrent
        L->>F: fetch(slot)
        F->>R: getBlock(slot, json, full, v1)
        alt block
            R-->>F: block
            F->>D: decode(block)
            D-->>W: rows per table, blocks(ok)
        else skipped slot (-32007 or -32009)
            F-->>W: blocks(skipped)
        else other error
            F->>F: backoff and retry
        end
    end
    W->>W: write tmp Parquet per table, fsync
    W->>W: rename into place
    W->>C: commit range (files, sha256, rows, decoder_version)
    L->>C: advance cursor
```

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> fetching : lane picks range
    fetching --> writing : all slots ok or skipped
    fetching --> pending : crash or provider outage, tmp files deleted on restart
    writing --> committed : files renamed, checkpoint row written in one transaction
    writing --> pending : crash before commit
    committed --> replaced : range re-run after decoder upgrade, old files removed after new commit
```

- `tail` records its start slot once, then follows finalized slots forward.
- `backfill` starts at that start slot and walks backward to `target_slot` (configured
  as a date, resolved to a slot once and stored).
- A range is never partially visible: files are written as `*.tmp`, synced, renamed,
  then recorded. Nothing is appended to an existing file.
- A slot is `skipped` only on the RPC's skipped-slot error codes. Any other failure
  retries; a range cannot commit with an unaccounted slot.

The checkpoint database holds one row per committed range: `lane`, `lo_slot`, `hi_slot`,
`status`, `decoder_version`, the file list with SHA-256 and row counts, `committed_at`.
It is operational state, not part of the dataset.

### Decoding a block

```mermaid
flowchart TD
    B[block JSON] --> T{for each transaction}
    T -->|"vote or meta.err"| DROP[drop]
    T -->|ok| P[collect programs of all outer and inner instructions]
    P -->|none in registry| DROP
    P -->|hit| I{for each venue instruction}
    I -->|tier 1| E[extract events for this ix_path]
    E --> E1["Program data: log lines, attributed by log nesting"]
    E --> E2["self-CPI event instructions (inner ix, program = venue, data prefix e445a52e51cb9a1d)"]
    E1 & E2 --> DIS{discriminator known and length matches?}
    DIS -->|yes| BOR["Borsh decode via IDL, add outer_program"] --> ROW["row in events/program/Event"]
    DIS -->|no| FAIL[decode_failures]
    I -->|tier 2| X[walk CPI subtree by stackHeight, collect transfers]
    X --> NET{nets to one trader, one mint in, one mint out?}
    NET -->|yes| ROW2["row in swaps/program"]
    NET -->|no| FAIL
    I -->|raydium_amm_v4| RAY["ray_log line, Borsh by variant, add amm"] --> ROW
    I -->|"dlmm initialize_position*"| ARGS["decode IDL args"] --> ROW3["row in instructions/meteora_dlmm/initialize_position"]
```

- Instruction paths: outer index `i`; inner instructions get `i.j`, `i.j.k`, … from
  `innerInstructions[].instructions[].stackHeight`, numbering siblings from 1.
- Log attribution: `Program X invoke [n]` and `Program X success` lines form a stack; a
  `Program data:` line belongs to the program on top and to the instruction executing
  at that depth.
- Log truncation (`Log truncated`) loses later log-emitted events; affected tier 1
  instructions get a `decode_failures` row with reason `log_truncated`. Self-CPI events
  are unaffected.
- `outer_program` is the program of the outer instruction `i` when it is not the venue
  itself.

### Rate limiting and provider

Token bucket per lane, in blocks per second (`config/collector.json`). 429 and timeouts
halve the rate temporarily and back off. The RPC URL is read from `.env`
(`SOLANA_RPC_URL`) and never logged. All reads use `finalized` commitment.

### Commands

| Command | Does |
| --- | --- |
| `fiber-data tail` | run the tail lane |
| `fiber-data backfill --until 2026-07-17` | run the backfill lane to that date |
| `fiber-data status` | cursors, ranges committed, rows per table, bytes per day |
| `fiber-data verify [--lane] [--from --to]` | slot coverage, file checksums, metadata |
| `fiber-data redecode --from --to` | re-run decoders on `decode_failures` rows |
| `fiber-data query --sql '...'` | DuckDB over the Parquet roots |
| `fiber-data registry check` | confirm every registry program exists on-chain and is executable |

## 6. Infrastructure and reproducibility

- Node 24, TypeScript, `@duckdb/node-api` for Parquet writing and queries, no ORM.
- Config in `config/` (venues, routers, collector rates and range size); secrets in
  `.env` (0600, git-ignored). IDLs vendored in `idl/` with source and version recorded in
  `idl/SOURCES.md`.
- `docker compose up -d` runs `tail` and `backfill` as two services sharing `data/` and
  `state/` volumes. Native run is `node dist/cli.js tail`.
- Moving to the larger machine: copy the repo, `data/` and `state/`, set the env file,
  start. Files are relative to the data root; no absolute paths are recorded.
- Decoder version is a constant in code, bumped whenever an IDL or decoder changes; it
  is written into every Parquet file's metadata and every checkpoint row.

## 7. Error handling

| Failure | Handling |
| --- | --- |
| RPC 429, timeout, 5xx | retry with exponential backoff, rate halves temporarily; range stays open |
| Skipped slot (-32007, -32009) | `blocks.status = skipped`; counts as accounted |
| Block not yet available (-32004) on tail | wait and retry; tail stays a small margin below the finalized tip |
| Borsh decode error, unknown discriminator, length mismatch | `decode_failures` row; range continues |
| Tier 2 call that does not net to a swap | `decode_failures` reason `not_a_swap`, transfers attached |
| Log truncated | `decode_failures` reason `log_truncated` for affected tier 1 instructions |
| Crash mid-range | tmp files deleted on restart; range redone from checkpoint |
| Disk full | writer fails before rename; nothing committed; process exits non-zero |
| Registry program not executable on-chain | `registry check` fails at startup |

## 8. Testing

- Fixture transactions as JSON in `tests/fixtures/`, fetched once, with expected rows
  asserted per decoder: every tier 1 event type at least once; tier 2 swaps through
  Jupiter and Titan; a Token-2022 `TransferChecked`; a native SOL leg; a self-CPI event
  and a log-emitted event; a log-truncated transaction; a version 1 transaction; a tier 2
  call that must land in `decode_failures` as `not_a_swap`; the three events that need
  the added `amm` or `pool_state` column; each DLMM `initialize_position*` variant.
- Range writer: kill mid-range, assert no registered file and no duplicate rows after
  resume.
- `verify` over a synthetic range containing a skipped slot and a retried fetch.
- IDL drift: a fixture whose event length differs from the IDL must land in
  `decode_failures`, not in an event table.
- Before the 90-day backfill: run `tail` for a few thousand slots and read `status` for
  measured bytes per day.

## 9. Sizing

Measured October 15, 2026 on 30 live blocks: 26% of non-vote transactions invoke a
registry program, about 32M per day. Estimated output 5–10 GB per day (wide events such
as PumpSwap `BuyEvent` dominate), under 1 TB per 90 days, to be confirmed by the first
live run. Backfill of about 19M slots at 10 blocks per second is about 22 days; the bound
is the RPC plan, not the code.

## 10. Open items for the implementation plan

- Verify on-chain, with `registry check`, every program ID in §3.
- Confirm the DBC IDL version bundled in the SDK matches the deployed program's event
  discriminators.
- Confirm the `ray_log` layouts and the `amm` account index per instruction against the
  current raydium-amm source.
- Measure actual bytes per day before launching the backfill.
