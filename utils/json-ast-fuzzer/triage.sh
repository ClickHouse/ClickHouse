#!/bin/bash
# Triage a crash/timeout artifact: recover the SQL, (for crashes) minimize, and dedup against known findings.
# Usage: triage.sh <path-to-artifact>   (crash-*/timeout-* file produced by a worker)
. "$(dirname "$(readlink -f "$0")")/env.sh"
A="${1:?artifact path}"; ART=$(readlink -f "$A"); ARGS=$(cat "$ARGS_FILE")
d=$(mktemp -d "$WORK/triage.XXXX"); cp "$ART" "$d/input"; cd "$d"
echo "== 1. load check (contention vs real): load $(cut -d' ' -f1 /proc/loadavg), free $(free -g|awk '/Mem:/{print $7}')G"
ps -eo pcpu,comm --sort=-pcpu | grep -v 'ps -eo' | head -3
echo "== 2. recover SQL (bounded replay; last_input is written before execution so it survives a hang):"
JSON_AST_FUZZER_LAST_INPUT="$d/last.txt" ASAN_SYMBOLIZER_PATH="$SYMBOLIZER" \
  timeout 45 "$EXEC_BIN" -runs=1 -detect_leaks=0 -timeout=40 input $ARGS > replay.log 2>&1
rc=$?; echo "replay exit $rc (71=completed/benign, 124/143=still hung -> likely real slow/hang)"
SQL=$(grep -A1 -- '--- SQL\|generated SQL' last.txt 2>/dev/null | tail -1)
echo "STUCK/CRASH SQL: $SQL"
echo "== 3. crash signature (if a sanitizer/LOGICAL_ERROR fired):"
grep -m1 -oE 'AddressSanitizer: [a-z-]+|ThreadSanitizer: [a-z-]+|runtime error: .*|LOGICAL_ERROR|Assertion .* failed|Received signal [0-9]+' replay.log | head -1
echo "== 4. minimize (crashes only; skip for timeouts):"
case "$(basename "$ART")" in
  crash-*) ASAN_SYMBOLIZER_PATH="$SYMBOLIZER" timeout 600 "$EXEC_BIN" -minimize_crash=1 -runs=20000 -detect_leaks=0 -timeout=40 -exact_artifact_path="$d/min" input $ARGS > minimize.log 2>&1; echo "minimized -> $d/min ($(wc -c < "$d/min" 2>/dev/null) bytes)";;
  *) echo "(timeout artifact; not minimizing)";;
esac
echo "== 5. dedup against known findings:"
K="$REPO/utils/json-ast-fuzzer/KNOWN_FINDINGS.md"
hit=$(grep -iFf <(printf '%s\n' "$SQL" | grep -oE '[a-zA-Z_]+' | sort -u | head -20) "$K" 2>/dev/null | head -3)
if [ -n "$hit" ]; then echo "POSSIBLE DUP of a known finding/class:"; echo "$hit"; else echo "no obvious match in KNOWN_FINDINGS.md — likely NEW; report via the findings channel."; fi
echo "== artifact workspace kept at $d"
