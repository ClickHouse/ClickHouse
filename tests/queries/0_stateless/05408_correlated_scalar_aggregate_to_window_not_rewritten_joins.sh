#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer for joins that leave the subquery
# not rewritten, mostly because no runtime filter restricts the window. The other cases are in
# `05385_correlated_scalar_aggregate_to_window_not_rewritten`.

create_tables t u x tp jt ku

check "PASTE JOIN" "SELECT n.number AS n, o.id AS id, (SELECT sum(v) FROM tp AS s WHERE s.k = o.k) AS c FROM (SELECT number FROM numbers(4)) AS n PASTE JOIN tp AS o WHERE n.number >= 0 AND o.k < 5 ORDER BY n"
check "joined on a column other than the key" "SELECT o.id AS id, o.v AS v, (SELECT max(v) FROM tp AS s WHERE s.id = o.id) AS c FROM tp AS o INNER JOIN u ON o.k = u.k WHERE u.w > 100 ORDER BY id"
check "joined on the key of only one of the windows" "SELECT o.id AS id, o.k AS k FROM tp AS o INNER JOIN u ON o.k = u.k WHERE u.w > 100 AND o.v >= (SELECT max(v) FROM tp AS s WHERE s.k = o.k) AND o.v <= (SELECT max(v) FROM tp AS s WHERE s.id = o.id) ORDER BY id"
check "condition on the key that also depends on the other table" "SELECT o.k AS k, o.v AS v, u.w AS w, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o INNER JOIN u ON o.k = u.k WHERE o.k < u.w ORDER BY k, v, w"
check "condition on a table that is not joined on the key" "SELECT u.k AS k, x.z AS z, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k CROSS JOIN x WHERE x.z > 3 ORDER BY k, z, v"
check "no runtime filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS enable_join_runtime_filters = 0"
check "join algorithm without runtime filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS join_algorithm = 'full_sorting_merge'"
check "ie_join first, no runtime filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS join_algorithm = 'ie_join,hash'"
check "condition on a table joined by lookups, no runtime filters" "SELECT o.k AS k, o.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o INNER JOIN jt ON o.k = jt.k WHERE jt.w > 0 ORDER BY k, v"
check "join keys without a common supertype, no runtime filters" "SELECT o.k AS k, o.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o INNER JOIN ku ON o.k = ku.k WHERE ku.w > 0 ORDER BY k, v"
check "another join key pair without a common supertype, no runtime filters" "SELECT o.k AS k, o.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o INNER JOIN ku ON o.k = ku.k32 AND toInt64(o.k) = ku.k WHERE ku.w > 0 ORDER BY k, v"
check "no join order optimization, the target may be on the build side" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS query_plan_optimize_join_order_limit = 0"
check "GLOBAL join, no runtime filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u GLOBAL INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v"
check "fixed choice of the join sides, the target may be on the build side" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS query_plan_join_swap_table = 0"

run_all

echo "--- materialized view reading its source table in the subquery ---"
# The subquery reads only the inserted block, as without the rewrite.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE mv_src (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mv_dst (k Int32, v Int32, c Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO mv_src VALUES (1, 100), (2, 200);
CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT o.k AS k, o.v AS v, (SELECT sum(v) FROM mv_src AS s WHERE s.k = o.k) AS c FROM mv_src AS o WHERE o.k < 3;
INSERT INTO mv_src SETTINGS optimize_correlated_scalar_aggregate_to_window = 1 VALUES (1, 1), (2, 2);
SELECT * FROM mv_dst ORDER BY k;
"
