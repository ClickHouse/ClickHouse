#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Open-loop mode of clickhouse-benchmark: queries arrive at random times at a fixed average rate.

echo "--- iterations: all arrivals are executed"
$CLICKHOUSE_BENCHMARK --arrival-rate 200 --seed 42 -i 50 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -oE '^Open-loop mode: .*|^Queries executed: [0-9]+|^Arrivals: [0-9]+, target rate: [0-9.]+/s'

echo "--- a list of rates makes one report per rate"
$CLICKHOUSE_BENCHMARK --arrival-rate 100,200 --seed 42 -i 20 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -oE '^Queries executed: [0-9]+|^Arrivals: [0-9]+, target rate: [0-9.]+/s'

echo "--- the arrivals that find all slots busy wait in the queue"
# 20 arrivals within about 20 ms, one slot, each query takes 50 ms: all but the first arrival wait.
$CLICKHOUSE_BENCHMARK --arrival-rate 1000 --max-in-flight 1 --seed 42 -i 20 -d 0 -q 'SELECT sleep(0.05)' 2>&1 \
    | grep -oE '^Queries executed: [0-9]+|^Arrivals: [0-9]+|queued: [0-9]+|max [0-9]+ of [0-9]+'

echo "--- the achieved rate does not change while the queries in flight finish"
# The arrivals stop at 0.3 seconds, the cumulative reports continue until the query in flight finishes at about 1 second.
$CLICKHOUSE_BENCHMARK --arrival-rate 1000 --max-in-flight 1 --seed 42 -t 0.3 -d 0.1 --cumulative -q 'SELECT sleep(1)' 2>&1 \
    | grep -oE 'achieved rate: [0-9.]+' | tail -2 | uniq | wc -l

echo "--- an early fatal error stops the run"
$CLICKHOUSE_BENCHMARK_BINARY --host 127.0.0.1 --port 1 --arrival-rate 1000000 --max-in-flight 256 --seed 42 -i 1 -d 0 -q 'SELECT 1' 2>&1 \
    | grep -m1 -oF 'Connection refused'

echo "--- per-query statistics"
printf 'SELECT 1\nSELECT 2\n' | $CLICKHOUSE_BENCHMARK --arrival-rate 200 --seed 42 -i 10 -d 0 --per-query 2>&1 \
    | grep -A 3 '^Per-query statistics' | grep -v '^Per-query' | cut -f 1,2,3,9

echo "--- errors"
$CLICKHOUSE_BENCHMARK --arrival-rate 10 -c 2 -q 'SELECT 1' 2>&1 | grep -oF 'cannot be used with --arrival-rate'
$CLICKHOUSE_BENCHMARK --arrival-rate 0 -q 'SELECT 1' 2>&1 | grep -oF 'must be a positive number'
$CLICKHOUSE_BENCHMARK --arrival-rate 1,x -i 1 -q 'SELECT 1' 2>&1 | grep -oF 'must be a positive number'
$CLICKHOUSE_BENCHMARK --arrival-rate 1,2 -q 'SELECT 1' 2>&1 | grep -oF 'requires --iterations or --timelimit'
$CLICKHOUSE_BENCHMARK --max-in-flight 2 -i 1 -q 'SELECT 1' 2>&1 | grep -oF 'requires --arrival-rate'
