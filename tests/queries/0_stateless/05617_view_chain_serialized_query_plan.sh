#!/usr/bin/env bash

# With `serialize_query_plan = 1`, the initiator asks a view read on a remote server whether it needs the whole query,
# which a view answers by traversing the views it reads. Each nested view was traversed again for every view above it,
# so a chain of N views took about 2^N lookups. A chain of 30 views without a table that needs the whole query is
# traversed once now, and the query is sent as a plan.
# The initiator decides it while planning, so `EXPLAIN` checks the long chain. A query is run on a short one only, as
# reading 30 nested views exceeds the stack allowed under TSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DEPTH=30

queries="DROP TABLE IF EXISTS view_chain_source; CREATE TABLE view_chain_source (x UInt64) ENGINE = MergeTree ORDER BY x;"
queries+="INSERT INTO view_chain_source SELECT number FROM numbers(100);"
queries+="CREATE VIEW view_chain_0 AS SELECT x FROM view_chain_source;"
for i in $(seq 1 ${DEPTH}); do
    queries+="CREATE VIEW view_chain_${i} AS SELECT x FROM view_chain_$((i - 1));"
done
${CLICKHOUSE_CLIENT} --multiquery -q "${queries}"

settings="SETTINGS enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, analyzer_inline_views = 0"

# A query that takes exponential time is stopped by `timeout`, and the test fails on its missing result.
timeout 120 ${CLICKHOUSE_CLIENT} --format Null -q "
    EXPLAIN SELECT count(), sum(x) FROM remote('127.0.0.2', currentDatabase(), 'view_chain_${DEPTH}') ${settings}" && echo "planned"

${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(x) FROM remote('127.0.0.2', currentDatabase(), 'view_chain_3') ${settings}"

queries=""
for i in $(seq ${DEPTH} -1 0); do
    queries+="DROP VIEW view_chain_${i};"
done
queries+="DROP TABLE view_chain_source;"
${CLICKHOUSE_CLIENT} --multiquery -q "${queries}"
