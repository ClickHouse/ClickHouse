-- A broadcast join keeps the partitioning of its left input also when that partitioning hashes the key at a
-- cast type. `f.k` is `Nullable(Int32)` and `g.k`, `h.k` are `Int32`, so both shuffle joins hash the key as
-- `Nullable(Int32)`. The broadcast of `dim` between them leaves the rows of `f JOIN g` where they are, so the join
-- with `h` needs no second shuffle of them.

SET explain_query_plan_default = 'legacy';
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
-- The written join order, so that the broadcast join with `dim` stays between the two shuffle joins.
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{
    "f":   { "cardinality": 1000000, "avg_row_bytes": 8, "distinct_keys": { "k": 1000000, "d": 10 } },
    "g":   { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "k": 1000000 } },
    "h":   { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "k": 1000000 } },
    "dim": { "cardinality": 10,      "avg_row_bytes": 4, "distinct_keys": { "id": 10 } }
}';

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS g;
DROP TABLE IF EXISTS h;
DROP TABLE IF EXISTS dim;

CREATE TABLE f (k Nullable(Int32), d Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE g (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE h (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE dim (id Int32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO f SELECT if(number % 7 = 0, NULL, number), number % 10 FROM numbers(1000);
INSERT INTO g SELECT number FROM numbers(1000);
INSERT INTO h SELECT number FROM numbers(1000);
INSERT INTO dim SELECT number FROM numbers(10);

EXPLAIN SELECT count() FROM f JOIN g ON f.k = g.k JOIN dim ON f.d = dim.id JOIN h ON h.k = f.k;

SELECT count() FROM f JOIN g ON f.k = g.k JOIN dim ON f.d = dim.id JOIN h ON h.k = f.k;
SELECT count() FROM f JOIN g ON f.k = g.k JOIN dim ON f.d = dim.id JOIN h ON h.k = f.k
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE f;
DROP TABLE g;
DROP TABLE h;
DROP TABLE dim;
