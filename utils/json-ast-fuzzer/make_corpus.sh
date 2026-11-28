#!/bin/bash
# Build/optimize the seed corpus for the execution fuzzer from the functional test suite, then split into 4 shards.
# Produces $WORK/shards/{0,1,2,3} (the per-shard read-only seed corpus used by run_round.sh) and a merged master.
# Usage: make_corpus.sh                (needs REF_CLICKHOUSE + CONVERTER_BIN + EXEC_BIN built)
set -e
. "$(dirname "$(readlink -f "$0")")/env.sh"
cd "$WORK" || exit 1
ARGS=$(cat "$ARGS_FILE"); GEN="$REPO/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/generate_seed_corpus.py"
mkdir -p seed_queries raw_seeds
# 1. collect statements from the stateless suite (and perf XML / docs if desired) into query files, ~2500 per part
echo "collecting statements from tests/queries ..."
: > seed_queries/all.sql
for f in "$REPO"/tests/queries/0_stateless/*.sql; do grep -hvE '^\s*--|^\s*$' "$f"; done >> seed_queries/all.sql 2>/dev/null || true
split -l 2500 -d -a 3 seed_queries/all.sql seed_queries/part
# 2. protobuf seeds per part (parseQueryToJSON -> protobuf) via the converter
echo "generating protobuf seeds ..."
ls seed_queries/part* | xargs -P 4 -I{} bash -c '
  p=$(basename {}); python3 -I "'"$GEN"'" --clickhouse "'"$REF_CLICKHOUSE"'" --converter "'"$CONVERTER_BIN"'" \
    --queries {} --output raw_seeds/$p > raw_seeds/$p.log 2>&1; echo "$p $(ls raw_seeds/$p 2>/dev/null | wc -l)"'
# 3. coverage-merge all raw seeds into a minimized master corpus (keeps only coverage-increasing inputs)
echo "coverage-merging into master ..."
mkdir -p corpus_master
ASAN_SYMBOLIZER_PATH="$SYMBOLIZER" UBSAN_SYMBOLIZER_PATH="$SYMBOLIZER" \
  "$EXEC_BIN" -merge=1 -detect_leaks=0 -timeout=60 -rss_limit_mb=8192 -max_len=65536 -artifact_prefix=./ \
  corpus_master $(ls -d raw_seeds/part* 2>/dev/null | tr '\n' ' ') $ARGS > corpus_master.merge.log 2>&1 || true
# 4. split master into 4 shards (round-robin) for the 4 workers
echo "splitting into 4 shards ..."
rm -rf shards; mkdir -p shards/0 shards/1 shards/2 shards/3
i=0; for x in corpus_master/*; do cp "$x" "shards/$((i%4))/" 2>/dev/null; i=$((i+1)); done
echo "done: master $(ls corpus_master|wc -l) seeds; shards 0..3 = $(for g in 0 1 2 3; do ls shards/$g|wc -l; done | tr '\n' '/')"
echo "seed round 0 corpora so the chain has a starting point:"
for s in a b c d; do mkdir -p json_ast_exec_corpus0$s; done
