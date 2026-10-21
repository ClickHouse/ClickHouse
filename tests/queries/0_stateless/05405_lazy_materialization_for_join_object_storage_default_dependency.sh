#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization over joins with an object storage table whose column has a `DEFAULT` computed from another one,
# under a join that reads nothing of its side: the join keeps a column of that side for the number of rows, and the
# column computed from it stays in the main read with it, so neither is read lazily, while the other side still is.
# The queries run with lazy materialization over joins enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_join_default"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data.parquet', ${AUTH}, 'Parquet') SELECT number AS k FROM numbers(10)
    SETTINGS s3_truncate_on_insert = 1;

    DROP TABLE IF EXISTS o;
    DROP TABLE IF EXISTS m;
    CREATE TABLE o (k UInt64, payload String DEFAULT toString(k)) ENGINE = S3('${URL}/data.parquet', ${AUTH}, 'Parquet');
    CREATE TABLE m (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO m SELECT number, concat('m', toString(number)) FROM numbers(5);
"

# Only the other side is sorted by, so that nothing of the object storage side is needed below the LIMIT. Which of its rows
# the LIMIT keeps is not known, so the result checks what holds for any of them.
QUERY="SELECT count(), min(payload = toString(k)), groupArray(heavy) FROM (
    SELECT o.k AS k, o.payload AS payload, m.k AS mk, m.heavy AS heavy FROM o CROSS JOIN m ORDER BY m.k LIMIT 3)"
SETTINGS="query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100, query_plan_optimize_lazy_materialization_for_object_storage = 1, enable_join_runtime_filters = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, cross_to_inner_join_rewrite = 0"

${CLICKHOUSE_CLIENT} --query "${QUERY} SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 1"
${CLICKHOUSE_CLIENT} --query "${QUERY} SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = 0"

# The heavy column of the other side is read lazily.
${CLICKHOUSE_CLIENT} --query "SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT o.k, o.payload, m.k, m.heavy FROM o CROSS JOIN m ORDER BY m.k LIMIT 3 SETTINGS ${SETTINGS})"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE o;
    DROP TABLE m;
"
