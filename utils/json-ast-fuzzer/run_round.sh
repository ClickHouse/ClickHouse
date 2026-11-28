#!/bin/bash
# One fuzzing round: 4 shard workers for <seconds>, each restarted from its own corpus until the deadline.
# Usage: run_round.sh <round> <exec-binary> <seconds> <shard0corpus> <shard1corpus> <shard2corpus> <shard3corpus>
. "$(dirname "$(readlink -f "$0")")/env.sh"
cd "$WORK" || exit 1
R=$1; BIN=$2; T=$3; shift 3; SRC=("$@")
ARGS=$(cat "$ARGS_FILE")
deadline=$(( $(date +%s) + T ))
for g in 0 1 2 3; do
  n=$(printf "\\x$(printf %x $((97+g)))"); d=exec_session$R$n; c=json_ast_exec_corpus$R$n
  mkdir -p "$d" "$c"
  ( cd "$d"; k=0
    while true; do
      left=$(( deadline - $(date +%s) )); [ $left -lt 300 ] && break
      k=$((k+1))
      ASAN_SYMBOLIZER_PATH="$SYMBOLIZER" UBSAN_SYMBOLIZER_PATH="$SYMBOLIZER" \
      timeout $((left+600)) "$BIN" -detect_leaks=0 -max_total_time=$left -timeout=120 -rss_limit_mb=12288 -max_len=65536 -print_final_stats=1 \
        -artifact_prefix=./ "$WORK/$c" "$WORK/${SRC[$g]}" "$WORK/shards/$g" $ARGS > run$k.log 2>&1
      echo "run $k exit: $? at $(date -u +%H:%M)" >> session.log
    done
    echo "exit: 0" >> session.log ) &
done
echo "round $R started $(date -u +%H:%M) with $BIN for $T s"
wait
