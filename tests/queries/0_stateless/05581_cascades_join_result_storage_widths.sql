-- Tags: no-fasttest
-- The width of a join result in Cascades comes from the storage widths of its columns, also when the filter above a
-- read renames them. Otherwise a short `String` counts with the default width of its type (64 bytes), and the bound of
-- the rows of `t JOIN k` (2000 rows) would not fit the broadcast budget below: the join with `b` would be a shuffle.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET distributed_plan_execute_locally = 1;
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
-- The estimates come from the column statistics that the inserts build.
SET use_statistics = 1;
SET min_marks_per_part_to_load_statistics = 0;
SET materialize_statistics_on_insert = 1;
-- A `Filter` step over the read of `t`, which renames its columns: their storage widths go through the rename.
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS w_big;
DROP TABLE IF EXISTS w_t;
DROP TABLE IF EXISTS w_k;
CREATE TABLE w_big (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'basic, uniq';
CREATE TABLE w_t (id UInt32, kind UInt8, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'basic, uniq';
CREATE TABLE w_k (id UInt8, name String) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'basic, uniq';
INSERT INTO w_big SELECT number % 1000, number FROM numbers(200000);
INSERT INTO w_t SELECT number, number % 2, 'x' FROM numbers(1000);
INSERT INTO w_k VALUES (0, 'a'), (1, 'b');

-- `LIKE` makes the estimates imprecise, so the broadcast budget counts the bound of the rows. 2000 rows of `t.s` and
-- `t.id` take about 26000 bytes from the storage widths, and about 136000 bytes with the type width of `String`.
-- Without the fixed cost of an exchange, the plan distributes these small tables.
SET distributed_plan_max_bytes_to_broadcast = 50000;
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead": 0}';
SELECT trimLeft(explain) FROM
(
    EXPLAIN SELECT count(), max(t.s) FROM w_big b JOIN w_t t ON b.id = t.id JOIN w_k k ON t.kind = k.id
    WHERE t.s LIKE '%x%' AND k.name LIKE '%a%'
)
WHERE explain LIKE '%HashJoin%' OR explain LIKE '%Join: %';

SELECT count(), max(t.s) FROM w_big b JOIN w_t t ON b.id = t.id JOIN w_k k ON t.kind = k.id
WHERE t.s LIKE '%x%' AND k.name LIKE '%a%';

DROP TABLE w_big;
DROP TABLE w_t;
DROP TABLE w_k;
