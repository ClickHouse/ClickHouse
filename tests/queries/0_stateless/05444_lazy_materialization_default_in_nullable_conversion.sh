#!/usr/bin/env bash

# A part that a pending `MODIFY COLUMN` from `Nullable` did not rewrite yet stores the column as `Nullable`, and the read
# fills its `NULL` values from the `DEFAULT`. With a non-deterministic default, a column whose default is computed from
# this one is not read lazily: the lazy read would compute it from other values than the ones the main read returns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, optimize_move_to_prewhere = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --alter_sync 0 --query "
    DROP TABLE IF EXISTS l;
    DROP TABLE IF EXISTS r;
    CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

    CREATE TABLE l (k UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO l SELECT number, NULL FROM numbers(1000);
    SYSTEM STOP MERGES l;
    ALTER TABLE l MODIFY COLUMN n UInt64 DEFAULT rand64();
    ALTER TABLE l ADD COLUMN s String DEFAULT toString(n);
"

for enabled in 1 0
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(n = toUInt64(s)) FROM (SELECT k, n, s FROM l WHERE n % 2 = 0 ORDER BY k LIMIT 20)
        SETTINGS ${SETTINGS}, query_plan_optimize_lazy_materialization = ${enabled}"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(n = toUInt64(s))
        FROM (SELECT l.n AS n, l.s AS s, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20)
        SETTINGS ${SETTINGS}, query_plan_lazy_materialization_for_join = ${enabled}"
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE l;
    DROP TABLE r;
"
