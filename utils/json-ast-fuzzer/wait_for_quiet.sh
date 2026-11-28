#!/bin/bash
# Block until a safe window for a heavy build on a SHARED box: enough free RAM AND no other agent mid-build.
# Load is intentionally ignored (co-tenant fuzzers keep it high). Usage: wait_for_quiet.sh [min_free_gb=50] [max_wait_sec=10800]
. "$(dirname "$(readlink -f "$0")")/env.sh"
min_free=${1:-50}; max_wait=${2:-10800}; deadline=$(( $(date +%s) + max_wait ))
while :; do
  free_g=$(free -g | awk '/Mem:/{print $7}')
  compilers=$(pgrep -c -x 'cc1plus|clang++-21|clang-21|rustc' 2>/dev/null); compilers=${compilers:-0}
  ninja_running=$(pgrep -c -x ninja 2>/dev/null); ninja_running=${ninja_running:-0}
  ts=$(date -u +%H:%M:%S)
  if awk -v f="$free_g" -v m="$min_free" 'BEGIN{exit !(f>=m)}' && [ "$compilers" -lt 4 ] && [ "$ninja_running" -eq 0 ]; then
    echo "$ts QUIET: free ${free_g}G, foreign-compilers ${compilers}, ninja ${ninja_running} -> go"; exit 0
  fi
  echo "$ts busy: free ${free_g}G(need ${min_free}) compilers ${compilers} ninja ${ninja_running} -> wait"
  [ "$(date +%s)" -ge "$deadline" ] && { echo "$ts TIMEOUT"; exit 1; }
  sleep 60
done
