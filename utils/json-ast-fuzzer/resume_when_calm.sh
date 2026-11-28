#!/bin/bash
# After pausing the chain during a co-tenant crisis (STOP_ROUNDS + killed workers), run this to auto-resume once the
# box is SUSTAINED-calm (load<100 & free>45G for 10 min), so it won't flap back during a brief dip.
. "$(dirname "$(readlink -f "$0")")/env.sh"
cd "$WORK" || exit 1
need=4; streak=0
for i in $(seq 1 600); do
  load1=$(cut -d' ' -f1 /proc/loadavg); free_g=$(free -g | awk '/Mem:/{print $7}')
  if awk -v l="$load1" -v f="$free_g" 'BEGIN{exit !(l<100 && f>45)}'; then streak=$((streak+1)); else streak=0; fi
  if [ "$streak" -ge "$need" ] && [ -f STOP_ROUNDS ]; then
    rm -f STOP_ROUNDS
    last=$(ls exec_session[0-9]*/session.log 2>/dev/null | xargs grep -l '^exit: 0' 2>/dev/null | sed -E 's#exec_session([0-9]+).*#\1#' | sort -n | tail -1); [ -z "$last" ] && last=0
    rm -rf exec_session$((last+1))? json_ast_exec_corpus$((last+1))? 2>/dev/null
    setsid nohup "$REPO/utils/json-ast-fuzzer/chain_rounds.sh" "$last" >> chain_rounds.log 2>&1 & disown
    echo "$(date -u) RESUMED from round $last" >> resume_when_calm.log; exit 0
  fi
  sleep 150
done
