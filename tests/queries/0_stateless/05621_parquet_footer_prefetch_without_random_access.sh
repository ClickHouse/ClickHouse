#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# A source of an object storage reads the Parquet footer of its next file in the background, while it reads the current one.
# A compressed file, or any file with `input_format_allow_seeks = 0`, is read into memory as a whole to get its footer,
# so it is not read in advance. The first row of two files must read as many bytes as the first file alone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}_"

# The second file is small, so that its background read finishes while the first file is still being read.
for ext in parquet parquet.zst; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/0.${ext}', format = Parquet) SELECT number AS x FROM numbers(1000000)"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/1.${ext}', format = Parquet) SELECT number AS x FROM numbers(1000)"
done

SETTINGS="--max_threads 1 --enable_parallel_replicas 0"
${CLICKHOUSE_CLIENT} $SETTINGS --query_id "${Q}zst_one" --query "SELECT x FROM s3(s3_conn, filename = '${DIR}/0.parquet.zst', format = Parquet, structure = 'x UInt64') LIMIT 1 FORMAT Null"
${CLICKHOUSE_CLIENT} $SETTINGS --query_id "${Q}zst_two" --query "SELECT x FROM s3(s3_conn, filename = '${DIR}/{0,1}.parquet.zst', format = Parquet, structure = 'x UInt64') LIMIT 1 FORMAT Null"
${CLICKHOUSE_CLIENT} $SETTINGS --input_format_allow_seeks 0 --query_id "${Q}no_seeks_one" --query "SELECT x FROM s3(s3_conn, filename = '${DIR}/0.parquet', format = Parquet, structure = 'x UInt64') LIMIT 1 FORMAT Null"
${CLICKHOUSE_CLIENT} $SETTINGS --input_format_allow_seeks 0 --query_id "${Q}no_seeks_two" --query "SELECT x FROM s3(s3_conn, filename = '${DIR}/{0,1}.parquet', format = Parquet, structure = 'x UInt64') LIMIT 1 FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

for name in zst no_seeks; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT anyIf(ProfileEvents['ParquetPrefetcherReadBytes'], query_id = '${Q}${name}_one')
            = anyIf(ProfileEvents['ParquetPrefetcherReadBytes'], query_id = '${Q}${name}_two')
        FROM system.query_log
        WHERE query_id IN ('${Q}${name}_one', '${Q}${name}_two') AND type = 'QueryFinish' AND current_database = currentDatabase()
    "
done
