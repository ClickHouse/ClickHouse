#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# A source of an object storage reads the Parquet footer of its next file in the background, while it reads the current one.
# One stream (`max_threads = 1`) reads all files, so every file but the first one is read through a prefetched footer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}_"

for i in 0 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/good/${i}.parquet', format = Parquet) SELECT number + ${i} * 100 AS x FROM numbers(100)"
done
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/bad/0.parquet', format = Parquet) SELECT number AS x FROM numbers(100)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/bad/1.parquet', format = RawBLOB) SELECT 'not a parquet file'"

SETTINGS="--max_threads 1 --use_parquet_metadata_cache 1"
${CLICKHOUSE_CLIENT} $SETTINGS --query_id "${Q}1" --query "SELECT count(x), sum(x), min(x), max(x) FROM s3(s3_conn, filename = '${DIR}/good/*.parquet')"
${CLICKHOUSE_CLIENT} $SETTINGS --query_id "${Q}2" --query "SELECT count(x), sum(x) FROM s3(s3_conn, filename = '${DIR}/good/*.parquet') WHERE x % 7 = 0"

# The file that is not Parquet fails the query as it did before the footer was read in advance.
${CLICKHOUSE_CLIENT} $SETTINGS --query "SELECT sum(x) FROM s3(s3_conn, filename = '${DIR}/bad/*.parquet')" 2>&1 | grep -o -m1 "INCORRECT_DATA"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# The second query finds all footers in the Parquet metadata cache.
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['ParquetMetadataCacheHits'] >= 4
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '${Q}2'
"
