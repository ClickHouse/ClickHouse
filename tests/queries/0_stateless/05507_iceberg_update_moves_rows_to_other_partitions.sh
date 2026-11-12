#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 101665: an UPDATE that moves the rows of one data file into several partitions writes each row only into the data file of
# its new partition: the rows, the record counts and the column bounds of the new files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/t"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t (p Int32, id Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY p;
    INSERT INTO t VALUES (0, 1), (0, 2), (0, 3);
    ALTER TABLE t UPDATE p = id WHERE p = 0;"

echo '--- rows'
${CLICKHOUSE_CLIENT} --query "SELECT p, id FROM t ORDER BY p, id"
echo '--- rows of partition 2'
${CLICKHOUSE_CLIENT} --query "SELECT id FROM t WHERE p = 2"
echo '--- rows, read without cached metadata'
${CLICKHOUSE_CLIENT} --query "SELECT p, id FROM icebergLocal('${TABLE_PATH}') ORDER BY p, id SETTINGS use_iceberg_metadata_files_cache = 0"
echo '--- added and total records of the UPDATE snapshot'
${CLICKHOUSE_CLIENT} --query "
    SELECT JSONExtractString(s, 'summary', 'added-records'), JSONExtractString(s, 'summary', 'total-records')
    FROM
    (
        SELECT arrayJoin(JSONExtractArrayRaw(json, 'snapshots')) AS s, JSONExtract(json, 'current-snapshot-id', 'Int64') AS current_id
        FROM (SELECT argMax(json, JSONExtract(json, 'last-sequence-number', 'Int64')) AS json
              FROM file('${TABLE_PATH}/metadata/*.metadata.json', 'JSONAsString'))
    )
    WHERE JSONExtract(s, 'snapshot-id', 'Int64') = current_id"
echo '--- data files pruned by the bounds of id'
${CLICKHOUSE_CLIENT} --enable_parallel_replicas=0 --use_iceberg_partition_pruning=1 \
    --query_id="${CLICKHOUSE_DATABASE}_minmax" --query "SELECT id FROM t WHERE id = 2 FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND query_id = '${CLICKHOUSE_DATABASE}_minmax'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
