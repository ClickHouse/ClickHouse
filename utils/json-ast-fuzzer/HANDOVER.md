# JSON-AST SQL fuzzer — handover

This directory is the self-contained harness for the structure-aware JSON-AST SQL fuzzer. Everything needed to run it
on a fresh node is on this branch (`json-ast-sql-parser-fuzzer`). The corpus is NOT in git (too large) — regenerate it
with `make_corpus.sh`.

Target audience: an agent that runs all day, has NO write access to the repo (public unauthenticated read of the fork
only), and shares findings through a separate channel (set up separately — do NOT git push).

## 0. What this fuzzer is

Two libFuzzer targets driven by a protobuf grammar (`src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast.proto`)
that mirrors ClickHouse's JSON AST representation. The mutator produces a protobuf, which is converted to a JSON AST,
then to SQL via `formatQueryFromJSON`:
- `json_ast_sql_parser_fuzzer` — parse/format/clone/hash + JSON writer round-trip. Aborts on a real defect
  (unexpected exception, non-canonical round trip under `JSON_AST_FUZZER_STRICT`, writer output the reader rejects).
- `json_ast_sql_execution_fuzzer` — runs each generated statement in-process via `clickhouse local` against a fixture
  schema (`programs/local/fuzzers/json_ast_sql_execution_fuzzer_schema.sql`), with a differential oracle comparing
  the result across plan variants (default / optimizations-off / parallel-external / JIT). Mismatches and asserts are
  findings. This is the flow the round chain runs continuously.

Source lives under `src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/` and `programs/local/fuzzers/`. This `utils/`
directory is only the orchestration + docs.

## 1. Status at handover (2026-11-05)

- Fuzzer binary generation is "v44": vocabulary refreshed (+165 functions, +210 settings in the proto). Branch tip
  carries all fixes below. Built as ASan+UBSan (`build_fuzz`, `SANITIZE=address,undefined`).
- Real bugs found & FIXED on the branch: findings 25,26,27,28,30 (see KNOWN_FINDINGS.md). Finding 31 (arithmetic-in-
  aggregate wrong result over empty input) is REAL but reported-not-fixed (maintainer decision).
- Campaign is deeply plateaued: no new real bug in ~160 exec rounds since finding 31. Recent signals are all accepted
  non-bug classes or benign slow-units/contention (KNOWN_FINDINGS.md).
- Shared box: a sibling `fuzz_lab` fleet + other servers run co-tenant and periodically oversubscribe CPU/RAM (load
  spikes to hundreds/thousands, RAM near-exhaustion). The harness auto-pauses/auto-restarts around this (sections 6-7).

## 2. One-time setup on a new node

```
git clone <public fork url>; cd <repo>; git checkout json-ast-sql-parser-fuzzer
# toolchain: clang-21 + clang++-21, cmake, ninja, llvm-symbolizer(-21), python3
. utils/json-ast-fuzzer/env.sh            # sets REPO, BUILD_FUZZ, WORK=$REPO/fuzz_work, etc. Override by exporting.
utils/json-ast-fuzzer/build_variants.sh asan_ubsan   # ~1-2h; builds the 3 binaries into build_fuzz
# a release reference build (for repro + seed generation):  cmake -B build -DSANITIZE= ...; ninja -C build clickhouse
utils/json-ast-fuzzer/make_corpus.sh      # regenerate seeds from the test suite -> $WORK/shards/{0..3}
```

## 3. Running the fuzzer (the round chain)

```
cd "$WORK"
setsid nohup utils/json-ast-fuzzer/chain_rounds.sh 0 >> "$WORK/chain_rounds.log" 2>&1 &
```
Each round = 4 shard workers for 6h, restarted from their own corpus on RSS-limit/timeout, carrying coverage forward.
Stop cleanly with `touch "$WORK/STOP_ROUNDS"`. Copy a newer exec binary into `$WORK/json_ast_sql_execution_fuzzer_vNN.bin`
and the chain picks it up at the next round.

## 4. Updating the fuzzer to learn new syntax

The mutator only emits function/setting names that are enumerated in `json_ast.proto`. When ClickHouse master gains
functions/settings, refresh the vocabulary:
```
utils/json-ast-fuzzer/refresh_vocabulary.sh            # dry run: prints how many functions/settings are missing
utils/json-ast-fuzzer/refresh_vocabulary.sh --apply    # appends the missing FN_/SET_ entries to json_ast.proto
utils/json-ast-fuzzer/build_variants.sh asan_ubsan     # proto changed -> rebuild (and rebuild any other variants in use)
```
New AST NODE types (not just functions) are deeper work: a new `ASTXxx` needs `writeJSON`/`readJSON` + a factory entry
in `src/Parsers/ASTFromJSON.cpp` + a `NodeType` in the proto, mirroring an existing node (e.g. how DescribeQuery /
ObjectTypeArgument were added). Data types / engines / table functions / combinators are free-form identifier strings,
no enum. To find NODE-type gaps: run the parser fuzzer with `JSON_AST_FUZZER_STRICT=json` over the corpus and read the
"JSON writer not supported" / rejections (see `src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/README.md`).
You have no repo write access: keep proto/source edits LOCAL (they still build & run locally); send anything worth
committing through the findings channel.

## 5. Building all binary variants

`build_variants.sh <variant>`: `asan_ubsan` (default, already running), `debug` (enables chassert/LOGICAL_ERROR aborts
that asan+ubsan — a `-O2 -DNDEBUG` build — compile out; but fuzzes at -O0 so far fewer execs/hour), `tsan`, `msan`,
`release`. Each is a full ClickHouse build (~2-3h, 50-100 GB). On a shared box ROTATE (one variant at a time into a
reused chain) rather than running all at once. ALWAYS before a build: `touch "$WORK/STOP_ROUNDS"` + kill workers to free
~40 GB, and `utils/json-ast-fuzzer/wait_for_quiet.sh` so you don't land a 32-compiler build on a co-tenant build spike
(that OOMs the box). After building, stage the binary (`cp .../json_ast_sql_execution_fuzzer $WORK/..._vNN.bin`),
`rm "$WORK/STOP_ROUNDS"`, restart the chain.

## 6. Collecting & optimizing the corpus

`make_corpus.sh` does the whole pipeline: extract statements from `tests/queries/0_stateless/*.sql` (add perf XML /
docs if you like), turn each into a protobuf seed via `generate_seed_corpus.py` + the converter, coverage-merge
(`-merge=1`, keeps only coverage-increasing inputs) into a minimized master, and round-robin split into 4 shards. To
re-optimize an existing corpus (dedup/minimize), just re-run the `-merge=1` step over the current shards into a fresh
dir. Keep `$WORK` disk under control: the chain accumulates `exec_session*/` dirs and old `*_vNN.bin` binaries (~4 GB
each) — delete old rounds and stale binaries (keep the newest exec binary + parser binary); `monitor_check_once.sh`
auto-deletes stale binaries on a disk>=97% ALERT.

## 7. Minimizing and deduplicating findings

`triage.sh <artifact>` does it end to end: (1) check load/top-CPU first — a timeout with the runner CPU-starved under
high load is co-tenant contention, not a bug; (2) recover the exact SQL via a bounded replay (`JSON_AST_FUZZER_LAST_INPUT`
is written before execution, so it survives a hang); (3) for `crash-*`, libFuzzer `-minimize_crash` to a small input;
(4) dedup by grepping the SQL tokens against `KNOWN_FINDINGS.md`. If it matches a known finding or accepted class, set
it aside; if not, it's likely NEW — share it (minimized input + SQL + sanitizer/stack signature + the `clickhouse local`
settings line) through the findings channel. Keep `KNOWN_FINDINGS.md` updated as you triage so dedup stays effective.
Report "exception"/LOGICAL_ERROR as that, not "crash" (release does not crash on logical errors).

Since 2026-11-05 the execution fuzzer writes `oracle_mismatches.log` and `json_ast_last_input.txt` under the
libFuzzer `-artifact_prefix` when one is given (`run_round.sh` passes `-artifact_prefix=./` inside the session
directory, so nothing moves for this harness); `JSON_AST_FUZZER_ORACLE_LOG` / `JSON_AST_FUZZER_LAST_INPUT` still
override. The same binaries also run in ClickHouse CI (`NightlyFuzzers` workflow, see the README section
"Continuous integration"), where the oracle log becomes a failing report row: a finding you see locally may show
up there as well, so check the nightly report before filing.

## 8. Monitoring loop (run all day)

Run `monitor_check_once.sh` on a schedule (e.g. every 2h). It prints only genuinely-new signals and is idempotent
(state in `$WORK/monitor_state/`):
- `ROUND:` — a round started; note it / tick your status channel.
- `ARTIFACT:` — a new crash/timeout; run `triage.sh` on it.
- `NEW ORACLE:` — a new mismatch signature outside the accepted classes; triage (reproduce on the release build; is
  the default-variant result actually wrong, or is it a dead-branch/resource/non-determinism class?).
- `ALERT: chain_rounds.sh not running` — the script auto-restarts it (unless `STOP_ROUNDS`). `ALERT: disk` — it frees
  stale binaries. For sustained co-tenant contention: `touch STOP_ROUNDS` + kill workers, then launch
  `resume_when_calm.sh` (auto-resumes after 10 min of sustained calm).
Nothing printed = healthy.

## 9. Gotchas (learned the hard way)
- Don't run a second heavy fuzzing flow (e.g. the parser session) alongside the exec chain on a shared box; it OOMs.
- Every `ninja -C build_fuzz` tends to be a near-full ~4700-object rebuild (shared tree invalidation) — pause the chain
  and wait_for_quiet first; the build survives OOM but background wait-tasks don't, so poll the log file directly.
- `pgrep -f X`/`pkill -f X` match your own shell; use the `[X]` bracket trick or kill by PID in a separate command.
- The exec binary is linked `--strip-debug`; libFuzzer `-print_coverage` drops functions without a source file — use a
  symbolizer wrapper that emits a placeholder filename if you want per-component coverage.
