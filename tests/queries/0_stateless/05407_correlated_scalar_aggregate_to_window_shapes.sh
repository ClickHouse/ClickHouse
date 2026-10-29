#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./correlated_scalar_aggregate_to_window.lib
. "$CUR_DIR"/correlated_scalar_aggregate_to_window.lib

# `optimize_correlated_scalar_aggregate_to_window` must not change any answer, for the shapes of the enclosing query.
# The other rewritten cases are in `05244_correlated_scalar_aggregate_to_window`.

create_tables t u x r tc ts

section "--- shapes of the enclosing query ---"
check "ORDER BY ALL shares the expression with the projection" "SELECT u.k AS k, t.v AS v, $SQ + 1 FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY ALL"
check "TPC-H Q17 shape" "SELECT sum(t.v) FROM u, t WHERE u.k = t.k AND u.w >= 100 AND t.v < (SELECT 0.5 * avg(v) FROM t AS s WHERE s.k = u.k)"
check "in WHERE with rows kept" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v >= (SELECT 0.5 * max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "two subqueries in one WHERE" "SELECT u.w AS w, t.v AS v FROM u, t WHERE u.k = t.k AND u.w > 100 AND t.v > (SELECT min(v) FROM t AS s WHERE s.k = u.k) AND t.v <= (SELECT max(v) FROM t AS s WHERE s.k = u.k) ORDER BY w, v"
check "correlated to the same table" "SELECT o.k AS k, o.v AS v, (SELECT count() FROM t AS s WHERE s.k = o.k) AS c FROM t AS o WHERE o.k > 1 ORDER BY k, v"
check "condition on the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.v > 5 AND u.w > 0 ORDER BY k, v"
check "condition on an ALIAS column of the correlated table" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE t.al < 100 AND u.w > 0 ORDER BY k, v"
check "join condition on a non-key column" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k AND t.v > u.w ORDER BY k, v"
# Which of the matching rows `ANY` keeps is not defined, so `t.v` is not selected; `c` is the same for all of them.
check "ANY join" "SELECT u.k AS k, $SQ AS c FROM u ANY INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k"
check "LEFT join, correlated table on the left" "SELECT o.k AS k, o.v AS v, u.w AS w, (SELECT max(v) FROM t AS s WHERE s.k = o.k) AS c FROM t AS o LEFT JOIN u ON u.k = o.k WHERE o.k > 0 ORDER BY k, v, w"
check "third table joined to the other relation" "SELECT u.k AS k, x.z AS z, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k INNER JOIN x ON u.w = x.w WHERE x.z > 1 ORDER BY k, z, v"
check "join_use_nulls" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS join_use_nulls = 1"
check "LIMIT" "SELECT c = multiIf(k = 1, 30, k = 2, 1e308, nan), v > 0 FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 LIMIT 1)"
check "IN subquery in WHERE" "SELECT count() FROM u, t WHERE u.k = t.k AND u.w IN (SELECT w FROM x) AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)"
check "non-deterministic condition" "SELECT countIf(c != multiIf(k = 1, 30, k = 2, 1e308, nan) OR v < -100) FROM (SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 AND rand(u.w) % 2 = 0)"
check "inside a correlated subquery" "SELECT o, (SELECT max(t.v) FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 100 AND z.o = 1 AND t.v >= (SELECT max(v) FROM t AS s WHERE s.k = u.k)) AS c FROM (SELECT number + 1 AS o FROM numbers(3)) AS z ORDER BY o"
check "FINAL on both" "SELECT u.k AS k, r.v AS v, (SELECT max(v) FROM r AS s FINAL WHERE s.k = u.k) AS c FROM u INNER JOIN r FINAL ON u.k = r.k WHERE u.w > 0 ORDER BY k, v"
check "additional_table_filters" "SELECT u.k AS k, t.v AS v, $SQ AS c FROM u INNER JOIN t ON u.k = t.k WHERE u.w > 0 ORDER BY k, v SETTINGS additional_table_filters = {'t': 'v < 20'}"
check "self-join" "SELECT a.k AS k, a.v AS v, (SELECT max(v) FROM t AS s WHERE s.k = a.k) AS c FROM t AS a INNER JOIN t AS b ON a.k = b.k WHERE a.k > 0 ORDER BY k, v"
check "column named like a window column" "SELECT o.k AS k, o.__correlated_aggregate_0 AS a, (SELECT max(__correlated_aggregate_0) FROM tc AS s WHERE s.k = o.k) AS c FROM tc AS o WHERE o.k > 0 ORDER BY k, a"
check "subcolumn" "SELECT o.k AS k, o.s AS s FROM ts AS o WHERE o.k > 0 AND length(o.s) * 2 < (SELECT sum(length(s)) FROM ts AS z WHERE z.k = o.k) ORDER BY k, s"
# The window reads the subcolumn `s.size` instead of the whole column. The plan is checked without parallel replicas,
# which describe the window step differently.
section "$($CLICKHOUSE_CLIENT --optimize_correlated_scalar_aggregate_to_window=1 --enable_parallel_replicas=0 --optimize_functions_to_subcolumns=1 -q "
    SELECT 'subcolumn: rewritten', countIf(explain LIKE '%Window step for window%'), 'reads the whole column', countIf(explain LIKE '%s String%') > 0
    FROM (EXPLAIN header = 1, actions = 1
    SELECT o.k AS k FROM ts AS o WHERE o.k > 0 AND length(o.s) * 2 < (SELECT sum(length(s)) FROM ts AS z WHERE z.k = o.k))")"

run_all
