#!/bin/bash
# One-shot scan for genuinely-new signals since last run (state in $WORK/monitor_state/). Silent if nothing new.
# Run it on a schedule (e.g. every 2h via cron) and triage whatever it prints (see HANDOVER.md "Monitoring").
. "$(dirname "$(readlink -f "$0")")/env.sh"
cd "$WORK" || exit 0
s=monitor_state; mkdir -p "$s"; touch "$s/seen_art" "$s/seen_oracle" "$s/seen_round"
L=$(tail -1 chain_rounds.log 2>/dev/null); [ -n "$L" ] && ! grep -qxF "$L" "$s/seen_round" && { echo "ROUND: $L"; echo "$L" >> "$s/seen_round"; }
for a in $(find exec_session[0-9]* -maxdepth 1 \( -name 'crash-*' -o -name 'timeout-*' \) 2>/dev/null); do
  b=$(basename "$a"); grep -qxF "$b" "$s/seen_art" && continue; echo "$b" >> "$s/seen_art"
  case "$b" in crash-da39a3ee*) continue;; esac           # empty-input artifact, benign
  d=$(dirname "$a"); grep -q 'out of memory: allocator is trying to allocate' "$d"/run*.log 2>/dev/null && continue  # huge-alloc, benign
  echo "ARTIFACT: $a"
done
for f in $(find exec_session[0-9]* -name 'oracle_mismatches.log' -size +1c 2>/dev/null); do
  grep -ohE '=== ORACLE (MISMATCH|ERROR ASYMMETRY)[^|]*' "$f" 2>/dev/null
done | grep -vE 'Code 403|Code 6: Cannot parse|Code 53: Cannot convert|caseWithExpression' \
     | sed 's/hash [0-9]*/hash N/g; s/rows [0-9]* vs [0-9]*/rows N vs N/g; s/[0-9]\+/N/g' | sort -u \
     | while read -r sig; do grep -qxF "$sig" "$s/seen_oracle" && continue; echo "$sig" >> "$s/seen_oracle"; echo "NEW ORACLE: $sig"; done
if ! pgrep -f '[c]hain_rounds.sh' >/dev/null; then
  echo "ALERT: chain_rounds.sh not running"
  if [ ! -f STOP_ROUNDS ]; then
    lastdone=$(ls exec_session[0-9]*/session.log 2>/dev/null | xargs grep -l "^exit: 0" 2>/dev/null | sed -E "s#exec_session([0-9]+).*#\1#" | sort -n | tail -1)
    if [ -n "$lastdone" ]; then
      rm -rf exec_session$((lastdone+1))? json_ast_exec_corpus$((lastdone+1))? 2>/dev/null
      setsid nohup "$REPO/utils/json-ast-fuzzer/chain_rounds.sh" "$lastdone" >> chain_rounds.log 2>&1 & disown
      echo "ALERT: auto-restarted chain from round $lastdone"
    fi
  fi
fi
used=$(df "$WORK" | tail -1 | awk '{print $5}' | tr -d '%'); [ "$used" -ge 97 ] && echo "ALERT: disk ${used}%"
exit 0
