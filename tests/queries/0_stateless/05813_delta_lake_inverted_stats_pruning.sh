#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# A Delta file whose stats are inverted (min > max) bounds no value, so the engine predicate must
# not skip it. `inverted.parquet` holds k = 1..5 with stats min 9 / max 1, `valid.parquet` holds
# k = 10..12 with correct stats and is still pruned. Each query prints its rows and scanned files.
# The Change Data Feed read (`delta_lake_snapshot_start_version`) takes the same predicate.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_inverted_stats"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${ROOT}/inverted.parquet', Parquet, 'k Int64') SELECT number + 1 FROM numbers(5);
    INSERT INTO FUNCTION file('${ROOT}/valid.parquet', Parquet, 'k Int64') SELECT number + 10 FROM numbers(3);
" < /dev/null

# The Change Data Feed reader opens each file with the size its `add` action records.
INVERTED_SIZE=$(stat -c%s "${ROOT}/inverted.parquet")
VALID_SIZE=$(stat -c%s "${ROOT}/valid.parquet")

# Hand-written Delta log, because ClickHouse's Delta writer emits no per-column stats.
mkdir -p "${ROOT}/_delta_log"
cat > "${ROOT}/_delta_log/00000000000000000000.json" <<JSON
{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}
{"metaData":{"id":"inverted","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"k\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"}}}
{"add":{"path":"inverted.parquet","partitionValues":{},"size":${INVERTED_SIZE},"modificationTime":0,"dataChange":true,"stats":"{\"numRecords\":5,\"minValues\":{\"k\":9},\"maxValues\":{\"k\":1},\"nullCount\":{\"k\":0}}"}}
{"add":{"path":"valid.parquet","partitionValues":{},"size":${VALID_SIZE},"modificationTime":0,"dataChange":true,"stats":"{\"numRecords\":3,\"minValues\":{\"k\":10},\"maxValues\":{\"k\":12},\"nullCount\":{\"k\":0}}"}}
JSON

Q="INSERT INTO results SELECT queryID(), arraySort(groupArray(k)) FROM deltaLakeLocal('${ROOT}')"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE results (query_id String, rows Array(Int64)) ENGINE = Memory;
    SET log_queries = 1;

    ${Q} WHERE k = 1;
    ${Q} WHERE k = 5;
    ${Q} WHERE k BETWEEN 1 AND 2;
    ${Q} WHERE k < 3;
    ${Q} WHERE k > 3;
    ${Q} WHERE k = 11;
    ${Q} WHERE k = 100;
    ${Q} WHERE k = 1 SETTINGS delta_lake_enable_engine_predicate = 0;
    ${Q} WHERE k = 1 SETTINGS delta_lake_snapshot_start_version = 0;

    SYSTEM FLUSH LOGS query_log;
    SELECT extract(query, ' WHERE ([^;]*)'), rows, ProfileEvents['DeltaLakeScannedFiles']
    FROM system.query_log JOIN results USING (query_id)
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND type = 'QueryFinish' AND query LIKE '%INSERT INTO results %'
    ORDER BY event_time_microseconds
    FORMAT TSVRaw;
" < /dev/null
