# How fiber-data works

Read the spec first: `docs/superpowers/specs/2026-10-15-fiber-data-collector-design.md`.
This page is the operational view: lanes, ranges, decoding, the trader rule, failures.

## Lanes and ranges

`tail` records the finalized slot it started at and follows new finalized slots forward,
staying `tipMargin` slots behind the tip. `backfill` starts one slot below the tail's
start slot and walks backward in ranges until `--until`. Each lane has its own checkpoint
database (`state/<lane>.duckdb`) and its own RPC rate from `config/collector.json`.

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

A range is written to `*.parquet.tmp` files, each fsynced and renamed into place, and only
then recorded in the checkpoint in one transaction. A crash before the commit leaves
temporary files that `tail` and `backfill` delete on start; the range is redone. A slot is
`skipped` only on the RPC's skipped-slot codes (-32007, -32009); every other error is
retried, so a range cannot commit with a hole.

## Decoding a block

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
    DIS -->|yes| BOR["Borsh decode via IDL, add outer_program and trader"] --> ROW["row in events/program/Event"]
    DIS -->|no| FAIL[decode_failures]
    I -->|tier 2| X[walk CPI subtree by stackHeight, collect transfers]
    X --> NET{nets to one trader, one mint in, one mint out?}
    NET -->|yes| ROW2["row in swaps/program"]
    NET -->|"no swap-shaped party"| DROP
    NET -->|ambiguous| FAIL
    I -->|raydium_amm_v4| RAY["ray_log line, Borsh by variant, add amm"] --> ROW
    I -->|"dlmm initialize_position*"| ARGS["decode IDL args"] --> ROW3["row in instructions/meteora_dlmm/initialize_position"]
    P -->|hit| O[for each outer instruction with venue calls] --> NET2{trader net: one mint in, one out, or one cycle mint?}
    NET2 -->|yes| ROW4["row in intents"]
    NET2 -->|no| FAIL
```

- Instruction paths: outer index `i` (0-based); inner instructions `i.j`, `i.j.k` from
  `stackHeight`, siblings numbered from 1.
- Events are taken from self-CPI instructions first, then from `Program data:` log lines;
  a log event equal in path, discriminator and payload to one already taken is dropped.
- `outer_program` is the top-level program when it differs from the venue.

### Trader rule

Applied to every venue instruction, tier 1 and tier 2 alike, using the SPL Token,
Token-2022 and System transfers inside the instruction's CPI subtree. Each token
account's owner and mint come from, in order: the block's token balance metadata; the
`InitializeAccount*` or associated-token-account creation instruction in the same
transaction (wrapped-SOL accounts created and closed inside a transaction never appear in
balance metadata); the transfer's own authority account. A venue instruction with no
transfers in its subtree (proprietary AMMs post on-chain price updates many times per
block this way) is neither a swap nor an intent and produces no row and no failure.

1. Net the transfers by token-account owner. The two parties of a swap are the owners
   that sent exactly one mint and received exactly one other mint.
2. The **user side** is the party that signed the transaction. If neither party signed
   (routers such as Jupiter's shared-accounts mode hold tokens in program-owned
   accounts during the hops), the user side is the party whose token accounts are also
   used by transfers outside this instruction; the pool's vaults are only ever touched
   inside it.
3. `trader` is the user-side owner when it signed. Otherwise it is the single signer
   whose transaction-level token balances changed. If there is no such signer, or more
   than one, `trader` is null.

Instructions with no transfers in their subtree (position creation, claims that go to a
third party, administrative events) get `trader` null; their IDL fields carry `owner`
where the program defines one.

Verified on October 16, 2026 against 82 Jupiter-routed hops in live blocks: 79 resolved
at step 2 by signature, 3 by the outside-use rule with the trader found at step 3, none
unresolved. The fee payer was the trader in most but not all of them.


## 7. Error handling

| Failure | Handling |
| --- | --- |
| RPC 429, timeout, 5xx | retry with exponential backoff, rate halves temporarily; range stays open |
| Skipped slot (-32007, -32009) | `blocks.status = skipped`; counts as accounted |
| Block not yet available (-32004) on tail | wait and retry; tail stays a small margin below the finalized tip |
| Borsh decode error, unknown discriminator, length mismatch | `decode_failures` row; range continues |
| Venue instruction or outer instruction with no transfers, or no swap-shaped party | skipped; counted in `status` |
| Tier 2 call with a swap-shaped party but no resolvable user side | `decode_failures` reason `not_a_swap`, transfers attached |
| Outer instruction with a swap-shaped party but unresolvable trader or net over more than two mints | `decode_failures` reason `not_an_intent`, transfers attached |
| Log truncated | `decode_failures` reason `log_truncated` for affected tier 1 instructions |
| Crash mid-range | tmp files deleted on restart; range redone from checkpoint |
| Disk full | writer fails before rename; nothing committed; process exits non-zero |
| Registry program not executable on-chain | `registry check` fails at startup |


## Commands

| Command | Does |
| --- | --- |
| `fiber-data tail [--max-ranges N]` | run the tail lane |
| `fiber-data backfill --until <YYYY-MM-DD or slot> [--max-ranges N]` | run the backfill lane |
| `fiber-data status` | cursors, ranges committed, rows per table |
| `fiber-data verify [--lane tail or backfill]` | range contiguity, file checksums, slot coverage |
| `fiber-data redecode [--from S --to S]` | re-run decoders on `decode_failures` rows with the current IDLs |
| `fiber-data query --sql '...'` | DuckDB over the Parquet roots; one view per table, e.g. `events__meteora_dlmm__Swap2Evt` |
| `fiber-data registry check` | confirm every registry program is executable on-chain |

## Smoke test on the development machine

Recorded after Task 12 of the implementation plan; see the section at the end of this file.

### Smoke test record, October 15, 2026 (this machine)

`rangeSlots` temporarily 50, `tail --max-ranges 2` against a Helius mainnet endpoint:

| Check | Result |
| --- | --- |
| `registry check` | all 109 tier 1 and tier 2 programs executable on-chain |
| `tail` | ranges 457347620–457347621 and 457347622–457347628 committed in 4.5 s wall time; 10,743 transactions seen, 1,242 touched a venue, 83 Parquet files |
| `verify --lane tail` | 2 ranges, 83 files, 0 problems |
| `intents` | 790 rows: 744 `swap`, 46 `cycle` |
| `decode_failures` | 70 rows: 40 Raydium CLMM `SwapEvent` length mismatches (16 bytes left after 12 fields: the deployed layout has grown past the vendored IDL, spec §10), 30 `not_an_intent` |
| size | 1.3 MB for 9 slots, which is per-file overhead at this range size (83 files for 9 slots); bytes per slot must be measured with the production 5,000-slot range before relying on it |
