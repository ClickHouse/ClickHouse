#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

READS=$(for _ in $(seq 1 200); do echo -n "SELECT count() FROM ${TABLE};"; done)

function read_table()
{
    ${CLICKHOUSE_CLIENT} --query "${READS}" 2>&1 | grep -v -x "1"
}

read_table &
read_table &
read_table &

for i in $(seq 1 20); do
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_compaction_delay_bias = ${i}"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_compaction_commit_batch_size = ${i}"
done

wait

${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${TABLE}" | grep -o -E "iceberg_compaction_(delay_bias|commit_batch_size) = [0-9]+"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
