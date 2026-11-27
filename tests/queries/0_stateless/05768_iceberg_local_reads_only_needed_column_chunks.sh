#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs the Avro and Parquet libraries, which the fast test does not build.
# An `IcebergLocal` data file is read as a local file, not with the gap-coalescing reads of remote storage:
# reading `a` and `b` around a ~2 MB column `pad` keeps prefetched bytes below 1.5x the bytes used.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `IcebergLocal` refuses a path outside the working directory of its `clickhouse local`.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_seek"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"

echo "
query_log:
    database: system
    table: query_log
" > "./${work_dir}/config.yaml"

${CLICKHOUSE_LOCAL} --config-file "./${work_dir}/config.yaml" --allow_insert_into_iceberg 1 --log_queries 1 --query "
    CREATE TABLE ice ENGINE = IcebergLocal('${work_dir}/icedata/')
    AS SELECT
        toInt64(number) AS a,
        concat(reinterpretAsString(sipHash128(number)), reinterpretAsString(sipHash128(number, 1))) AS pad,
        toInt64(number) AS b
    FROM numbers(60000);
    SELECT sum(a), sum(b) FROM ice FORMAT Null SETTINGS log_comment = 'read_a_b';
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['ParquetPrefetcherReadBytes'] < 1.5 * ProfileEvents['ParquetPrefetcherUsedBytes']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'read_a_b';"

rm -rf "./${work_dir}"
