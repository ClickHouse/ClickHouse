#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-parallel-replicas
# Tag no-fasttest: delta-kernel-rs and MinIO are not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
# Tag no-parallel-replicas: counts the S3 requests of a query, and under parallel replicas they are made by other replicas

# Reading a `DeltaLake` table makes no object-metadata request (S3 HEAD) for its Parquet data files: they are never
# overwritten, and the `add` action of every file records its size. The table function and a read of `_etag` keep
# the request. Each INSERT adds one data file, so the requests of a read are compared between 3 and 6 files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_TEST_UNIQUE_NAME}/t0"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}_"
DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"

${CLICKHOUSE_CLIENT} $DELTA_SETTINGS --query "CREATE TABLE t0 (c0 Int32, c1 String) ENGINE = DeltaLakeS3(s3_conn, filename = '${TABLE_PATH}')"

insert()
{
    for i in "$@"; do
        ${CLICKHOUSE_CLIENT} $DELTA_SETTINGS --query "INSERT INTO t0 VALUES ($i, toString($i))"
    done
}

read_all()
{
    local settings="--allow_experimental_delta_kernel_rs 1 --s3_validate_etag_on_read 1"
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}a_$1" --query "SELECT sum(c0) FROM t0"
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}b_$1" --query "SELECT sum(c0) FROM deltaLakeS3(s3_conn, filename = '${TABLE_PATH}')"
    ${CLICKHOUSE_CLIENT} $settings --query_id "${Q}c_$1" --query "SELECT countIf(_etag = '') FROM t0"
}

insert 1 2 3
read_all 3
insert 4 5 6
read_all 6

${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs 1 --query "SELECT countIf(_time IS NULL) FROM t0"

# The filesystem cache and the Parquet metadata cache keep serving the data files without the requests.
CACHE_SETTINGS="--allow_experimental_delta_kernel_rs 1 --filesystem_cache_name cache_for_readbigat --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 --use_columns_cache 0 --use_parquet_metadata_cache 1 --enable_filesystem_cache 1"
${CLICKHOUSE_CLIENT} $CACHE_SETTINGS --query_id "${Q}r1" --query "SELECT sum(c0) FROM t0"
${CLICKHOUSE_CLIENT} $CACHE_SETTINGS --query_id "${Q}r2" --query "SELECT sum(c0) FROM t0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT q, sumIf(head, files = 6) - sumIf(head, files = 3)
    FROM
    (
        SELECT
            splitByChar('_', substring(query_id, length('${Q}') + 1)) AS parts,
            parts[1] AS q,
            toUInt32(parts[2]) AS files,
            ProfileEvents['S3HeadObject'] AS head
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND startsWith(query_id, '${Q}') AND match(substring(query_id, length('${Q}') + 1), '^[abc]_[36]$')
    )
    GROUP BY q
    ORDER BY q
"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        anyIf(ProfileEvents['S3HeadObject'], query_id = '${Q}r2'),
        anyIf(ProfileEvents['CachedReadBufferReadFromCacheBytes'], query_id = '${Q}r2') > 0,
        anyIf(ProfileEvents['ParquetMetadataCacheHits'], query_id = '${Q}r2') > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id IN ('${Q}r1', '${Q}r2')
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t0"
