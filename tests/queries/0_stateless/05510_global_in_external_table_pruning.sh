#!/usr/bin/env bash
# Tags: no-fasttest, shard
# Tag no-fasttest: needs the IcebergLocal engine and Parquet.
# Tag shard: uses `remote('127.0.0.{1,2}', ...)`.

# A `GLOBAL IN` set over an external table (`_data_...` on a shard of a distributed query, a client external
# table, a temporary table) prunes the files and row groups of Iceberg and File(Parquet) reads, as it prunes
# the parts of a MergeTree table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"
EXT_FILE=$(mktemp)
trap 'rm -rf "${TABLE_PATH}" "${EXT_FILE}"' EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
# Four data files, with the ids 0-999, 1000-1999, 2000-2999 and 3000-3999.
for i in 0 1 2 3; do
    $CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number + ${i} * 1000 FROM numbers(1000)"
done
# The same files with Avro as the configured format: manifest pruning does not depend on it.
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${TABLE}_avro ENGINE = IcebergLocal('${TABLE_PATH}/', 'Avro') SETTINGS allow_experimental_iceberg_compaction = 0"
# One file with four row groups.
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${TABLE}_file (id Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_row_group_size = 1000"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${TABLE}_file SELECT number FROM numbers(4000)"

# A serialized query plan gives the shard its sets without their source, so they are not built early there.
function remote_query()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), sum(id) FROM remote('127.0.0.{1,2}', currentDatabase(), $1)
        WHERE id GLOBAL IN (SELECT 1500 UNION ALL SELECT 1501)
        SETTINGS prefer_localhost_replica = $2, serialize_query_plan = 0, log_comment = '${CLICKHOUSE_DATABASE}_$3'"
}

remote_query "${TABLE}" 0 iceberg_r0
remote_query "${TABLE}" 1 iceberg_r1
remote_query "${TABLE}_avro" 0 iceberg_avro
remote_query "${TABLE}_file" 0 file_r0
remote_query "${TABLE}_file" 1 file_r1

printf '1500\n1501\n' > "${EXT_FILE}"
$CLICKHOUSE_CLIENT --external --file="${EXT_FILE}" --name=ext --structure='id Int64' --query "
    SELECT count(), sum(id) FROM ${TABLE} WHERE id GLOBAL IN ext SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_ext'"

$CLICKHOUSE_CLIENT --query "
    CREATE TEMPORARY TABLE tmp (id Int64);
    INSERT INTO tmp VALUES (1500), (1501);
    SELECT count(), sum(id) FROM ${TABLE} WHERE id GLOBAL IN tmp SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_tmp';"

# Without pruning each read gets all 4000 rows of the table, once per shard.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', '') AS arm, read_rows < if(arm IN ('ext', 'tmp'), 2000, 4000)
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
        AND current_database = currentDatabase() AND startsWith(log_comment, '${CLICKHOUSE_DATABASE}_')
    ORDER BY arm"

$CLICKHOUSE_CLIENT --query "DROP TABLE ${TABLE}_file"
$CLICKHOUSE_CLIENT --query "DROP TABLE ${TABLE}_avro"
$CLICKHOUSE_CLIENT --query "DROP TABLE ${TABLE}"
