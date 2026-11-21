#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/78795
# An `ATTACH` with the full definition can bring a new table, so its key is checked like that of a `CREATE`: a key over a
# column that merges aggregate is rejected unless the definition sets `allow_aggregating_columns_in_partition_or_order_key`.
# A short `ATTACH` of an existing table is covered by `05457`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Replicated` database prints the status of each DDL query otherwise; the server warns that an `ATTACH` with the full
# definition is not recommended.
DDL_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --send_logs_level fatal"

# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
function uuid_clause()
{
    if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
        echo "UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
    fi
}

DEFINITION="(k UInt8, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree PARTITION BY c % 2 ORDER BY k"

echo "--- rejected without the setting"
${DDL_CLIENT} --query "ATTACH TABLE t_full_attach $(uuid_clause) ${DEFINITION}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_full_attach'"

echo "--- attached with the setting"
${DDL_CLIENT} --query "ATTACH TABLE t_full_attach $(uuid_clause) ${DEFINITION} SETTINGS allow_aggregating_columns_in_partition_or_order_key = 1"
${CLICKHOUSE_CLIENT} --query "SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_full_attach'"

${DDL_CLIENT} --query "DROP TABLE t_full_attach"
