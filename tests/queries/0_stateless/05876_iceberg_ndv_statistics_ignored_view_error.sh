#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and DataSketches
# A sink of a parallel insert into the Iceberg target of a materialized view gets an error, which is ignored with
# `materialized_views_ignore_errors`. The rows it has written are not committed, so their values do not get into
# the NDV statistics of the snapshot either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query()
{
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --iceberg_write_table_statistics_ndv=1 "${@:2}" -q "$1" -- --user_files_path="${WORK_DIR}"
}

# Small blocks spread the insert over the parallel sinks; the sink which gets the block with 777 fails.
local_query "
    CREATE TABLE source (k Int64) ENGINE = Memory;
    CREATE TABLE target (k Int64) ENGINE = IcebergLocal('${WORK_DIR}/target/', 'Parquet');
    CREATE MATERIALIZED VIEW mv TO target AS SELECT k FROM source WHERE NOT throwIf(k = 777);
    INSERT INTO source SELECT number FROM numbers_mt(1000);" \
    --materialized_views_ignore_errors=1 --parallel_view_processing=1 --max_threads=4 --max_insert_threads=4 \
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 \
    --max_block_size=100 --max_insert_block_size=100 --min_insert_block_size_rows=100 --min_insert_block_size_bytes=1 \
    --send_logs_level=fatal

# A part of the rows is committed, and the statistics of the snapshot count their values only.
# Below 4096 distinct values the sketch is exact.
metadata=$(find "${WORK_DIR}/target/metadata/" -name '*.metadata.json' | sort -V | tail -n 1)
local_query "
    CREATE TABLE target ENGINE = IcebergLocal('${WORK_DIR}/target/', 'Parquet');
    WITH
        (SELECT json FROM file('${metadata}', 'JSONAsString')) AS m,
        arrayFilter(e -> JSONExtractInt(e, 'snapshot-id') = JSONExtractInt(m, 'current-snapshot-id'), JSONExtractArrayRaw(m, 'statistics')) AS entries,
        toUInt64OrZero(JSONExtractString(JSONExtractArrayRaw(entries[1], 'blob-metadata')[1], 'properties', 'ndv')) AS ndv
    SELECT count() < 1000, uniqExact(k) = ndv FROM target"
