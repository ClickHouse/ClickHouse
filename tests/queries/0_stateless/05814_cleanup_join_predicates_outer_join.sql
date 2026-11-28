-- An outer join fills the columns of an unmatched side with defaults or NULLs, so an equality of columns of that side
-- does not hold above the join. The join order optimizer must not remove a predicate as made redundant by it.

SET enable_analyzer = 1;
SET joined_subquery_requires_alias = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;
SET use_statistics = 0;

CREATE TABLE o_l (l Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE o_a (a Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE o_b (b Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE o_c (c Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO o_l VALUES (1);
INSERT INTO o_a VALUES (0);
INSERT INTO o_b VALUES (0);
INSERT INTO o_c VALUES (0);

-- `o_l` matches no row of `o_a JOIN o_b`, so the row has `a = 0` and `b = NULL`: `b = c` is false.
SELECT 'left join, join_use_nulls = 0', count()
FROM o_l LEFT ALL JOIN (o_a INNER ALL JOIN o_b ON a = b) ON l = a INNER ALL JOIN o_c ON a = c AND b = c
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_conflict_detector = 'c', join_use_nulls = 0;
SELECT 'left join, join_use_nulls = 1', count()
FROM o_l LEFT ALL JOIN (o_a INNER ALL JOIN o_b ON a = b) ON l = a INNER ALL JOIN o_c ON a = c AND b = c
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_conflict_detector = 'c', join_use_nulls = 1;
SELECT 'left join, written order', count()
FROM o_l LEFT ALL JOIN (o_a INNER ALL JOIN o_b ON a = b) ON l = a INNER ALL JOIN o_c ON a = c AND b = c
SETTINGS query_plan_optimize_join_order_limit = 0, join_use_nulls = 0;
