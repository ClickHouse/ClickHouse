#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query result previews of `INSERT ... RETURNING` with the data sent by the client: the previews of
# the aggregation in `RETURNING` must not become rows of the result, and the client must accept them
# both while it is still sending the data and after it has sent all of it.

SETTINGS="SETTINGS query_result_previews = 1, query_result_previews_min_interval_ms = 0, max_threads = 1, max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_previews_returning"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_previews_returning (x UInt64) ENGINE = Null"

seq 1 100000 | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_previews_returning RETURNING (SELECT sum(x), count()) ${SETTINGS} FORMAT TSV"
seq 1 100000 | ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT sum(x), count()) ${SETTINGS} FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_previews_returning"
