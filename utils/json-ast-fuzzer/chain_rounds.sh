#!/bin/bash
# Chain rounds forever: when round N finishes (all 4 shards exit:0), start round N+1 from N's corpora with the
# newest exec binary found in $WORK. Stop by `touch $WORK/STOP_ROUNDS`. Usage: chain_rounds.sh <starting-round>
. "$(dirname "$(readlink -f "$0")")/env.sh"
cd "$WORK" || exit 1
RUN_ROUND="$REPO/utils/json-ast-fuzzer/run_round.sh"
N=$1
while true; do
  until [ "$(ls exec_session${N}?/session.log 2>/dev/null | xargs grep -l '^exit: 0' 2>/dev/null | wc -l)" -ge 4 ]; do sleep 120; done
  [ -f STOP_ROUNDS ] && { echo "stopped before round $((N+1))"; exit 0; }
  # newest locally-staged exec binary (copy new builds into $WORK as json_ast_sql_execution_fuzzer_vNN.bin)
  BIN=$(ls -t "$WORK"/json_ast_sql_execution_fuzzer_v*.bin 2>/dev/null | head -1); [ -z "$BIN" ] && BIN="$EXEC_BIN"
  M=$((N+1))
  "$RUN_ROUND" $M "$BIN" 21600 json_ast_exec_corpus${N}a json_ast_exec_corpus${N}b json_ast_exec_corpus${N}c json_ast_exec_corpus${N}d >> chain_rounds.log 2>&1
  N=$M
done
