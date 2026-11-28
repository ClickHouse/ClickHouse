#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds, and `IcebergLocal` needs
# the USE_AVRO build option

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `max_parsing_threads = 1` the `Vortex` reader runs its tasks inside `read`. A `Parquet`
# reader that initializes the shared resources of the snapshot first still sets up a download
# pool, and the `Vortex` reader must not schedule its reads there: `read` would not see a read in
# flight on that pool and would report the scan as stuck.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05407_vortex_iceberg_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# `clickhouse-local` takes its working directory for the user files directory.
cd "${LOCAL_DIR}" || exit 1

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE t (a Int64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Parquet');
    INSERT INTO t SELECT number, toString(number) FROM numbers(2000000);
"

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE t ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');
    INSERT INTO t SELECT number, toString(number) FROM numbers(2000000, 2000000);
"

for parsing_threads in 1 2; do
    ${CLICKHOUSE_LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Parquet');
        SELECT count(), sum(a), max(s) FROM t
        SETTINGS max_threads = 1, max_parsing_threads = $parsing_threads, max_download_threads = 4, input_format_parquet_enable_row_group_prefetch = 1;"
done
