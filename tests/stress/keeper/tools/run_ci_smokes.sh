#!/usr/bin/env bash
set -euo pipefail

# Simple CI smoke runner for Keeper stress tests.
# - Runs CHA-02/CHA-03/CHA-05 with short duration and optional seed sweep
# - Writes JSONL sidecar and prints a short summary at the end
# Usage:
#   KEEPER_SEEDS="1,2,3" ./tests/stress/keeper/tools/run_ci_smokes.sh

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
export PYTHONPATH="${REPO_ROOT}"
: "${KEEPER_SCENARIO_FILE:=cha.yaml}"
: "${KEEPER_DURATION:=20}"
: "${KEEPER_MONITOR_INTERVAL_S:=5}"
: "${KEEPER_PROM_EVERY_N:=3}"
: "${KEEPER_METRICS_FILE:=/tmp/keeper_metrics.jsonl}"
: "${KEEPER_SEEDS:=1}"

SCEN_FILTER='test_scenario and (CHA-02 or CHA-03 or CHA-05)'

# Clean sidecar
rm -f "${KEEPER_METRICS_FILE}" || true

# Run matrix (seeds)
OLD_IFS="$IFS"; IFS=',' read -r -a SEEDS_ARR <<<"${KEEPER_SEEDS}"; IFS="$OLD_IFS"
for s in "${SEEDS_ARR[@]}"; do
  echo "[keeper][ci] running smokes with seed=${s}"
  SEED_ENV=("KEEPER_SEED=${s}")
  env "${SEED_ENV[@]}" \
  pytest -q -k "${SCEN_FILTER}" tests/stress/keeper/tests/test_scenarios.py -s || true
done

# Summary
python3 tests/stress/keeper/tools/summarize_jsonl.py "${KEEPER_METRICS_FILE}" || true
