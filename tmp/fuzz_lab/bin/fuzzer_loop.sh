#!/bin/bash
# Per-instance fuzzer loop. Expects these env vars (set by harness.py):
#   PORT           — TCP port of the target server
#   INST_DIR       — instance directory (tmp/fuzz_lab/inst/NN)
#   BIN            — full path to clickhouse binary
#   CORPUS         — path to the curated oracle_merged_queries.sql
#   STATELESS_DIR  — path to tests/queries/0_stateless (random-sample source)
#   FLEET_DIR      — tmp/fuzz_lab (for state/mismatches.persistent.log)
#   RAM_CAP_BYTES  — client-side memory cap
#
# Batch model (2026-06-09 redesign):
#   * --ignore-error: a failing seed no longer aborts the batch; previously
#     every batch died at the first broken seed and most of the corpus
#     never executed.
#   * Oracle mismatches are detected by grepping the client output for the
#     error text, NOT via exit codes: AST_FUZZER_ORACLE_MISMATCH is error
#     code 905 and the plain client exits 905 % 256 = 137, which collides
#     with SIGKILL/OOM. (The _exit(49) convention only exists in the
#     client-side fuzz loop, which we don't use.)
#   * Corpus blocks (curated / oracle-shapes / each stateless file) are
#     shuffled per batch and the batch is bounded by BATCH_TIMEOUT, so over
#     many batches every part of the corpus gets fuzzed instead of only the
#     head of a fixed-order file.
#   * Per-batch randomization mirroring CI: random session settings prelude,
#     random --compatibility (flips hundreds of setting defaults), and
#     occasional --ast_fuzzer_any_query=1 (DDL/INSERT fuzzing).

set -u

: "${PORT:?}"
: "${INST_DIR:?}"
: "${BIN:?}"
: "${CORPUS:?}"
: "${FLEET_DIR:?}"
: "${RAM_CAP_BYTES:?5368709120}"
: "${STATELESS_DIR:=/home/ubuntu/clickhouse/tests/queries/0_stateless}"

: "${BATCH_FILES:=40}"           # random stateless tests per batch
: "${AST_FUZZER_RUNS:=100}"      # fuzz mutants per seed query
: "${BATCH_TIMEOUT:=2400}"       # seconds before a batch is cut (timeout(1))
: "${BATCHES_PER_RESTART:=10}"   # server recycle cadence (see below)

ORACLE_SHAPES=/home/ubuntu/clickhouse/tmp/fuzz_lab/templates/oracle_shapes.sql
POPULATE=/home/ubuntu/clickhouse/tmp/fuzz_lab/templates/populate.sql
POPULATE_RICH=/home/ubuntu/clickhouse/tmp/fuzz_lab/templates/populate_rich.sql

STATUS_LOG="$INST_DIR/status.log"
RUNS_DIR="$INST_DIR/logs/runs"
SHARDS_DIR="$INST_DIR/logs/shards"
mkdir -p "$RUNS_DIR" "$SHARDS_DIR"

RUN=$(wc -l < "$STATUS_LOG" 2>/dev/null || echo 0)

# Pick one item from the arguments uniformly at random.
rnd_pick() { local n=$#; local i=$((RANDOM % n + 1)); echo "${!i}"; }

# Emit a random settings prelude: each pooled setting is included with
# probability 1/3, with a random value. All of these only change HOW a query
# executes, never the correct result, so oracle comparisons (which run
# within the same session) stay valid.
emit_settings_prelude() {
    (( RANDOM % 3 == 0 )) && echo "SET max_threads = $(rnd_pick 1 2 4);"
    (( RANDOM % 3 == 0 )) && echo "SET max_block_size = $(rnd_pick 31 100 1024 8192 65536);"
    (( RANDOM % 3 == 0 )) && echo "SET join_algorithm = '$(rnd_pick hash parallel_hash grace_hash partial_merge full_sorting_merge auto)';"
    (( RANDOM % 3 == 0 )) && echo "SET group_by_two_level_threshold = $(rnd_pick 1 100000);"
    (( RANDOM % 3 == 0 )) && echo "SET group_by_two_level_threshold_bytes = $(rnd_pick 1 50000000);"
    (( RANDOM % 3 == 0 )) && echo "SET optimize_aggregation_in_order = $(rnd_pick 0 1);"
    (( RANDOM % 3 == 0 )) && echo "SET optimize_read_in_order = $(rnd_pick 0 1);"
    (( RANDOM % 3 == 0 )) && echo "SET max_bytes_before_external_group_by = $(rnd_pick 0 1000000);"
    (( RANDOM % 3 == 0 )) && echo "SET max_bytes_before_external_sort = $(rnd_pick 0 1000000);"
    (( RANDOM % 3 == 0 )) && echo "SET compile_expressions = $(rnd_pick 0 1);"
    (( RANDOM % 3 == 0 )) && echo "SET min_count_to_compile_expression = $(rnd_pick 0 3);"
    (( RANDOM % 3 == 0 )) && echo "SET optimize_move_to_prewhere = $(rnd_pick 0 1);"
    (( RANDOM % 3 == 0 )) && echo "SET use_uncompressed_cache = $(rnd_pick 0 1);"
    return 0
}

while true; do
    [ -f "$INST_DIR/.stopped" ] && { echo "[$(date +%H:%M:%S)] .stopped marker set; exiting loop" >> "$STATUS_LOG"; exit 0; }

    if ! "$BIN" client --port "$PORT" -q "SELECT 1" >/dev/null 2>&1; then
        echo "[$(date +%H:%M:%S)] server not up" >> "$STATUS_LOG"
        sleep 5
        continue
    fi

    RUN=$((RUN + 1))
    OUT="$RUNS_DIR/run_${RUN}.log"

    # ---- Build this batch's corpus -------------------------------------
    # populate.sql and the settings prelude always come first; after that,
    # the curated corpus, the oracle-shapes corpus, and each random
    # stateless file are independent blocks concatenated in random order.
    SHARD_FILE="$SHARDS_DIR/shard_${RUN}.sql"
    SHARD_LIST="$SHARDS_DIR/shard_${RUN}.list"

    PRELUDE=$(emit_settings_prelude)
    {
        echo "-- prelude"
        echo "$PRELUDE"
        cat "$POPULATE"
        [ -f "$POPULATE_RICH" ] && cat "$POPULATE_RICH"
    } > "$SHARD_FILE"

    find "$STATELESS_DIR" -maxdepth 1 -type f -name '*.sql' 2>/dev/null \
        | sort -R | head -n "$BATCH_FILES" > "$SHARD_LIST"
    [ -f "$CORPUS" ] && echo "$CORPUS" >> "$SHARD_LIST"
    [ -f "$ORACLE_SHAPES" ] && echo "$ORACLE_SHAPES" >> "$SHARD_LIST"

    sort -R "$SHARD_LIST" | while IFS= read -r f; do
        echo "-- $f"
        cat "$f"
        echo
    done >> "$SHARD_FILE"

    # ---- Per-batch client knobs ----------------------------------------
    EXTRA_FLAGS=()
    KNOBS="runs=$AST_FUZZER_RUNS"
    if (( RANDOM % 5 < 2 )); then
        COMPAT=$(rnd_pick 24.3 24.8 24.12 25.3 25.8 26.3)
        EXTRA_FLAGS+=("--compatibility=$COMPAT")
        KNOBS="$KNOBS compat=$COMPAT"
    fi
    # any_query batches fuzz DDL/INSERT/ALTER too. Async side effects
    # (mutations with mutations_sync=0, inserts) can land BETWEEN the
    # oracle's reference and rewritten executions, so result comparison is
    # meaningless there — any_query batches hunt crashes, oracle stays off.
    ORACLE_FLAG="--ast_fuzzer_oracle=1"
    if (( RANDOM % 4 == 0 )); then
        EXTRA_FLAGS+=("--ast_fuzzer_any_query=1")
        ORACLE_FLAG="--ast_fuzzer_oracle=0"
        KNOBS="$KNOBS any_query=1 oracle=0"
    fi
    [ -n "$PRELUDE" ] && KNOBS="$KNOBS prelude=$(echo "$PRELUDE" | grep -c 'SET')"

    timeout -k 10 "$BATCH_TIMEOUT" "$BIN" client --port "$PORT" \
        --max_execution_time=10 \
        --max_memory_usage="$RAM_CAP_BYTES" \
        --stacktrace \
        --ignore-error \
        --ast_fuzzer_runs="$AST_FUZZER_RUNS" \
        "$ORACLE_FLAG" \
        "${EXTRA_FLAGS[@]}" \
        --queries-file "$SHARD_FILE" \
        > "$OUT" 2>&1
    EC=$?
    echo "[$(date +%H:%M:%S)] Run=$RUN exit=$EC $KNOBS files=$(wc -l < "$SHARD_LIST" 2>/dev/null || echo ?)" >> "$STATUS_LOG"

    # ---- Oracle-mismatch detection (grep, NOT exit codes) --------------
    MISM_COUNT=$(grep -ac "oracle mismatch" "$OUT" 2>/dev/null | head -1)
    MISM_COUNT=${MISM_COUNT:-0}
    if [ "$MISM_COUNT" -gt 0 ]; then
        BLOCK=$(grep -aB 2 -A 40 "Code: 905" "$OUT" | head -200)
        # Known-family tagging keeps the persistent log triage-able.
        FAMILY="UNCLASSIFIED"
        # Classify ONLY against the actual oracle query lines, not the whole
        # 200-line block: the block includes ~40 trailing lines of stack trace
        # and the next query's trace, whose stray keywords (FINAL, system.,
        # WITH FILL, ...) otherwise mis-tag an unrelated real mismatch as a
        # "known" family and hide it. These labels bound the compared queries.
        QLINES=$(echo "$BLOCK" | grep -aE '(Reference|Partitioned|Wrapped|Optimized|Unoptimized|Metamorphic|Default|With|variant) ')
        echo "$QLINES" | grep -aq "WITH TOTALS" && FAMILY="known-totals-leak (tmp/totals_leak_repro)"
        echo "$QLINES" | grep -aq "remove_redundant_distinct\|WITH CUBE\|WITH ROLLUP\|GROUPING SETS" && FAMILY="known-distinct-cube (tmp/distinct_cube_repro)"
        echo "$QLINES" | grep -aq "hasToken\|hasAllTokens\|hasAnyTokens\|hasPhrase\|searchAny\|searchAll" && FAMILY="known-hastoken-textindex (tmp/hastoken_null_repro)"
        echo "$QLINES" | grep -aqE "\bnan\b|t_minmax_float|negate_nan|_r_minmax" && FAMILY="known-nan-minmax (tmp/nan_minmax_repro)"
        echo "$QLINES" | grep -aq "map_test_index\|bf_tokenbf_map\|mapKeys\|mapValues" && FAMILY="known-mapbloom (tmp/mapkeys_bloom_repro)"
        echo "$QLINES" | grep -aqE "(RIGHT|LEFT|FULL) JOIN" && echo "$QLINES" | grep -aq "AS y) AS b ON y = x\|SELECT 1, \*" && FAMILY="known-outerjoin-const (tmp/rightjoin_const_repro #106923)"
        echo "$QLINES" | grep -aq "QUALIFY" && echo "$QLINES" | grep -aq "RIGHT JOIN" && FAMILY="known-joinengine-qualify (tmp/joinengine_qualify_repro)"
        echo "$QLINES" | grep -aq "timeSeries" && FAMILY="known-timeseries-aggregate (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aq "WITH FILL" && FAMILY="known-withfill (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aq "REPLACE (" && FAMILY="known-replace-alias (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aqE " FINAL( |$|\))" && FAMILY="known-final-nondistributive (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aqE "mannWhitneyUTest|studentTTest|welchTTest|meanZTest|kolmogorovSmirnovTest|rankCorr|theilsU|cramersV|contingency|categoricalInformationValue" && FAMILY="known-stat-test-aggregate (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aqE "system\.|INFORMATION_SCHEMA\." && FAMILY="known-systemtable-cte (oracle gate, fixed in source)"
        echo "$QLINES" | grep -aq "dist_\|Distributed" && FAMILY="known-distributed (oracle gate, fixed in source)"
        # A batch can produce several distinct mismatches; the single FAMILY
        # above is last-match-wins, so a NEW family co-occurring with a known
        # one would be hidden. Guard against that: if any reference/optimized
        # mismatch query matches NONE of the known family keywords, force
        # UNCLASSIFIED so the batch is always investigated.
        KNOWN_RE='WITH TOTALS|remove_redundant_distinct|WITH CUBE|WITH ROLLUP|GROUPING SETS|hasToken|hasAllTokens|hasAnyTokens|hasPhrase|searchAny|searchAll|nan|t_minmax_float|negate_nan|_r_minmax|map_test_index|bf_tokenbf_map|mapKeys|mapValues|(RIGHT|LEFT|FULL) JOIN|QUALIFY|timeSeries|WITH FILL|REPLACE \(| FINAL|mannWhitneyUTest|studentTTest|welchTTest|meanZTest|kolmogorovSmirnovTest|rankCorr|theilsU|cramersV|contingency|categoricalInformationValue|system\.|INFORMATION_SCHEMA\.|dist_|Distributed'
        if echo "$QLINES" | grep -aE '(Reference|Optimized|Unoptimized) query' | grep -avqE "$KNOWN_RE"; then
            FAMILY="UNCLASSIFIED (mixed: had $FAMILY + an unrecognized mismatch)"
        fi
        {
            echo "=== MISMATCH inst=$(basename "$INST_DIR") run=$RUN time=$(date +%Y-%m-%dT%H:%M:%S) count=$MISM_COUNT family=$FAMILY knobs=[$KNOBS] ==="
            echo "$BLOCK"
            echo ""
        } >> "$INST_DIR/mismatches.log"
        (
            flock -x 200
            {
                echo "=== MISMATCH inst=$(basename "$INST_DIR") run=$RUN time=$(date +%Y-%m-%dT%H:%M:%S) count=$MISM_COUNT family=$FAMILY knobs=[$KNOBS] ==="
                echo "$BLOCK"
                echo ""
            } >> "$FLEET_DIR/state/mismatches.persistent.log"
        ) 200> "$FLEET_DIR/state/mismatches.persistent.log.lock"
        # Preserve the full batch artifacts for any unclassified family.
        if [ "$FAMILY" = "UNCLASSIFIED" ]; then
            KEEP_DIR="$INST_DIR/mismatch_artifacts/run_${RUN}"
            mkdir -p "$KEEP_DIR"
            cp "$OUT" "$SHARD_FILE" "$KEEP_DIR/" 2>/dev/null || true
        fi
    fi

    # Rotate run logs + shard artifacts: keep only the most recent 100/50.
    ls -t "$RUNS_DIR"/run_*.log 2>/dev/null | tail -n +101 | xargs -r rm -f
    ls -t "$SHARDS_DIR"/shard_*.sql 2>/dev/null | tail -n +51 | xargs -r rm -f
    ls -t "$SHARDS_DIR"/shard_*.list 2>/dev/null | tail -n +51 | xargs -r rm -f

    # Every BATCHES_PER_RESTART batches, recycle the *server* (not this
    # fuzzer loop — that would suicide). Exercises the server-start path and
    # clears accumulated server-side state.
    if (( BATCHES_PER_RESTART > 0 )) && (( RUN % BATCHES_PER_RESTART == 0 )); then
        echo "[$(date +%H:%M:%S)] rotation-restart: drain + recycle server" >> "$STATUS_LOG"

        "$BIN" client --port "$PORT" \
            --connect_timeout 2 --receive_timeout 5 \
            -q "KILL QUERY WHERE 1 SYNC" >/dev/null 2>&1 || true

        SRV_PID=$(cat "$INST_DIR/server.pid" 2>/dev/null || echo "")
        if [ -n "$SRV_PID" ] && kill -0 "$SRV_PID" 2>/dev/null; then
            kill -TERM "$SRV_PID" 2>/dev/null || true
            for i in $(seq 1 30); do
                kill -0 "$SRV_PID" 2>/dev/null || break
                sleep 1
            done
            if kill -0 "$SRV_PID" 2>/dev/null; then
                kill -KILL "$SRV_PID" 2>/dev/null || true
                echo "[$(date +%H:%M:%S)] rotation-restart: SIGKILL (drain+SIGTERM did not exit in 30s)" >> "$STATUS_LOG"
            fi
        fi

        if [ -n "${HARNESS_PY:-}" ] && [ -n "${INST_N:-}" ]; then
            if [ -n "${RUN_ID:-}" ]; then
                python3 "$HARNESS_PY" cycle start --inst "$INST_N" --run "$RUN_ID" >> "$STATUS_LOG" 2>&1 || true
            else
                python3 "$HARNESS_PY" start --inst "$INST_N" >> "$STATUS_LOG" 2>&1 || true
            fi
            echo "[$(date +%H:%M:%S)] rotation-restart: done" >> "$STATUS_LOG"
        else
            echo "[$(date +%H:%M:%S)] rotation-restart: server killed but no HARNESS_PY/INST_N to restart — relying on watchdog" >> "$STATUS_LOG"
        fi
    fi
done
