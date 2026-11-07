#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs the Avro and Parquet libraries, which the fast test does not build.

# A read of an object storage table function uses the userspace page cache by default, when the server
# has one: `use_page_cache_for_object_storage` is enabled by default. The same table is read twice with
# no cache-related settings, and the second read must be served from the page cache.
#
# The CI server configures no page cache (`page_cache_max_size` is 0 by default), so everything runs in a
# private clickhouse-local that has one, as in 05332_iceberg_local_page_cache_random_read.sh. The random
# settings of tests/clickhouse-test are passed only to clickhouse-client, so clickhouse-local sees the defaults.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `IcebergLocal` refuses a path outside the working directory of its `clickhouse local`.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"

> "./${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
query_log:
    database: system
    table: query_log
"

${CLICKHOUSE_LOCAL} --path "./${work_dir}/db" --config-file "./${work_dir}/config.yaml" --allow_insert_into_iceberg 1 --log_queries 1 --query "
    CREATE TABLE ice ENGINE = IcebergLocal('${work_dir}/icedata/')
    AS SELECT toInt64(number) AS k, repeat('x', 200) AS v FROM numbers(200000);
    SELECT value FROM system.settings WHERE name = 'use_page_cache_for_object_storage';
    SELECT count(), sum(k) FROM icebergLocal('${work_dir}/icedata/', 'Parquet') SETTINGS log_comment = 'cold';
    SELECT count(), sum(k) FROM icebergLocal('${work_dir}/icedata/', 'Parquet') SETTINGS log_comment = 'warm';
    SYSTEM FLUSH LOGS query_log;
    -- The cold read populates the page cache, and the warm read is served from it.
    SELECT log_comment, if(log_comment = 'cold', ProfileEvents['PageCacheMisses'], ProfileEvents['PageCacheHits']) > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('cold', 'warm')
        ORDER BY event_time_microseconds;"

rm -rf "./${work_dir}"
