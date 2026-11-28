#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A full-definition ATTACH is checked like CREATE: a key expression cannot have the name of a stored column.

db_engine=$(${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")
uuid_clause() { [ "${db_engine}" = "Ordinary" ] || echo "UUID '$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")'"; }

${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_bad $(uuid_clause) (x String, \`lower(x)\` String)
    ENGINE = MergeTree ORDER BY lower(x) SETTINGS allow_suspicious_indices = 0; -- { serverError BAD_ARGUMENTS }"

${CLICKHOUSE_CLIENT} --send_logs_level=error -q "ATTACH TABLE t_ok $(uuid_clause) (x String, \`lower(x)\` String)
    ENGINE = MergeTree ORDER BY lower(x) SETTINGS allow_suspicious_indices = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_ok'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ok"
