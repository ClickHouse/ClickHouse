#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly PID=$$
readonly TEST_USER=$"02834_USER_${PID}"

#${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=${TEST_USER}&password=pass" \
#   -d "SELECT * FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', 'system', 'one', '${TEST_USER}', 'pass')"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE db;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS db.table;"

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS db.dict_name;"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE db.table (label_data_combination_id UInt64) engine = MergeTree Order BY label_data_combination_id;"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${TEST_USER};"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON db.* TO ${TEST_USER};"
#${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON db.* TO ${TEST_USER};"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE ON db.* TO ${TEST_USER};"
#${CLICKHOUSE_CLIENT} -q "GRANT DROP ON db.* TO ${TEST_USER};"

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=${TEST_USER}&password=pass" \
 -d "CREATE OR REPLACE DICTIONARY db.dict_name
    (
      label_data_combination_id UInt64
    )
    PRIMARY KEY label_data_combination_id
    SOURCE(CLICKHOUSE(
      QUERY
        'SELECT
          label_data_combination_id
        FROM db.table'
      )
    )
    LAYOUT(HASHED(SHARDS 16 SHARD_LOAD_QUEUE_BACKLOG 10000))
    LIFETIME(0)
    SETTINGS(dictionary_use_async_executor = 1, max_threads = 16)
    COMMENT 'Dictionary mapping of label_data_combination_id to the underlying data.';"


${CLICKHOUSE_CLIENT} -q  "DROP TABLE IF EXISTS db.table;"
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS db.dict_name;"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE db;"
