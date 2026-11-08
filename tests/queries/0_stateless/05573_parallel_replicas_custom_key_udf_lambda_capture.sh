#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL UDF in the custom key for parallel replicas keeps its meaning when a lambda parameter in its body
# has the same name as the column passed to it: `g(y)` is `y + 1`, not the constant `2`.
# SQL UDFs are global, so the function name is made unique per test database.

UDF="g_capture_${CLICKHOUSE_DATABASE}"
SETTINGS="parallel_replicas_mode = 'custom_key_sampling', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_count = 2, parallel_replicas_custom_key = '${UDF}(y)'"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_05573;
    DROP FUNCTION IF EXISTS ${UDF};

    CREATE TABLE t_05573 (y UInt64) ENGINE = MergeTree ORDER BY y;
    INSERT INTO t_05573 SELECT number FROM numbers(10);

    CREATE FUNCTION ${UDF} AS x -> arraySum(arrayMap(y -> x + y, [1]));

    SELECT groupArray(${UDF}(y)) FROM t_05573;
    SELECT groupArray(y) FROM (SELECT y FROM t_05573 ORDER BY y) SETTINGS ${SETTINGS}, parallel_replica_offset = 0;
    SELECT groupArray(y) FROM (SELECT y FROM t_05573 ORDER BY y) SETTINGS ${SETTINGS}, parallel_replica_offset = 1;

    DROP TABLE t_05573;
    DROP FUNCTION ${UDF};
"
