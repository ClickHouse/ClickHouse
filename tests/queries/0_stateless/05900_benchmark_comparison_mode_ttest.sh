#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-benchmark runs Student's t-test only in comparison mode, i.e. when two
# --host/--port endpoints are given, and reports the verdict at the level selected by
# --confidence (0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%, default 5).
# Expected behaviour source: docs/concepts/features/tools-and-utilities/clickhouse-benchmark.mdx,
# sections "Comparison mode" ("specify endpoints of both servers by two pairs of --host, --port
# keys"), the --confidence option and the example report, which ends with the line
# "No difference proven at 99.5% confidence".
#
# Both endpoints are the same test server, so which of the two verdicts the t-test reaches
# depends on the measured latencies. Asserted is the documented part: in comparison mode
# there is exactly one verdict line, at the requested confidence level.

HOST="${CLICKHOUSE_HOST:-localhost}"
PORT="${CLICKHOUSE_PORT_TCP:-9000}"
VERDICT='^(No difference proven at|Difference at) '

# Comparison mode at 95% confidence: one verdict line, reported at 95%.
$CLICKHOUSE_BENCHMARK --host="$HOST" --host="$HOST" --port="$PORT" --confidence=2 \
    --iterations=40 --concurrency=1 --delay=0 --query='SELECT 1' 2>&1 |
    grep -cE "${VERDICT}95% confidence"

# Comparison mode with the default confidence level: one verdict line, reported at 99.5%.
$CLICKHOUSE_BENCHMARK --host="$HOST" --host="$HOST" --port="$PORT" \
    --iterations=40 --concurrency=1 --delay=0 --query='SELECT 1' 2>&1 |
    grep -cE "${VERDICT}99\.5% confidence"

# One endpoint is not comparison mode: there is no second sample and no verdict at all.
# The absence of a verdict only means something if the run itself succeeded, so the exit code
# and the complete report of all 40 iterations are asserted before the missing verdict.
SINGLE_OUT=$($CLICKHOUSE_BENCHMARK --iterations=40 --concurrency=1 --delay=0 --query='SELECT 1' 2>&1)
echo "exit code: $?"
echo "$SINGLE_OUT" | grep -c 'Queries executed: 40 (100%)\.'
echo "$SINGLE_OUT" | grep -c 'confidence' ||:
