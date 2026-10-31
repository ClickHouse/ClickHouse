#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Open-loop mode of clickhouse-benchmark: queries arrive at random times at a fixed average rate.
# The seed fixes the arrival schedule, and the test checks only the values that do not depend on timing.

echo "--- iterations: all arrivals are executed"
$CLICKHOUSE_BENCHMARK --arrival-rate 200 --seed 42 -i 50 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -oE '^Open-loop mode: .*|^Queries executed: [0-9]+|^Arrivals: [0-9]+, target rate: [0-9.]+/s'

echo "--- a list of rates makes one report per rate"
$CLICKHOUSE_BENCHMARK --arrival-rate 100,200 --seed 42 -i 20 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -oE '^Queries executed: [0-9]+|^Arrivals: [0-9]+, target rate: [0-9.]+/s'

echo "--- at most --max-in-flight queries execute at the same time, the queued arrivals are executed too"
$CLICKHOUSE_BENCHMARK --arrival-rate 1000 --max-in-flight 1 --seed 42 -i 20 -d 0 -q 'SELECT sleep(0.01)' 2>&1 \
    | grep -oE '^Queries executed: [0-9]+|^Arrivals: [0-9]+|max [0-9]+ of [0-9]+'

echo "--- an early fatal error stops the run"
$CLICKHOUSE_BENCHMARK_BINARY --host 127.0.0.1 --port 1 --arrival-rate 1000000 --max-in-flight 256 --seed 42 -i 1 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -m1 -oF 'Connection refused'

echo "--- per-query statistics"
printf 'SELECT 1\nSELECT 2\n' | $CLICKHOUSE_BENCHMARK --arrival-rate 200 --seed 42 -i 10 -d 0 --per-query 2>&1 \
    | grep -A 3 '^Per-query statistics' | grep -v '^Per-query' | cut -f 1,2,3,10

echo "--- errors"
$CLICKHOUSE_BENCHMARK --arrival-rate 10 -c 2 -q 'SELECT 1' 2>&1 | grep -oF 'cannot be used with --arrival-rate'
$CLICKHOUSE_BENCHMARK --arrival-rate 0 -q 'SELECT 1' 2>&1 | grep -oF 'must be a positive number'
$CLICKHOUSE_BENCHMARK --arrival-rate 1,x -i 1 -q 'SELECT 1' 2>&1 | grep -oF 'must be a positive number'
$CLICKHOUSE_BENCHMARK --arrival-rate 1,2 -q 'SELECT 1' 2>&1 | grep -oF 'requires --iterations or --timelimit'
$CLICKHOUSE_BENCHMARK --max-in-flight 2 -i 1 -q 'SELECT 1' 2>&1 | grep -oF 'requires --arrival-rate'
