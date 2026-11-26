-- A `LEFT` or `RIGHT` join is removed if no column of its other side is read
-- and it outputs every row of the preserved side exactly once.
-- Each case shows the number of joins left in the plan and, where a join would change it, the result.

SET enable_analyzer = 1;
SET query_plan_remove_unused_joins = 1;
SET query_plan_remove_unused_columns = 1; -- the columns of the other side that only a removed join read are pruned by it
SET explain_query_plan_default = 'legacy'; -- one line per step
SET enable_parallel_replicas = 0; -- the joins of the plan are counted
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim;
DROP VIEW IF EXISTS fact_named;

CREATE TABLE fact (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dim (k UInt64, k2 UInt32, name String) ENGINE = MergeTree ORDER BY k;
CREATE VIEW fact_named AS SELECT fact.id AS id, dim.name AS name FROM fact LEFT ANY JOIN dim ON fact.k = dim.k;

INSERT INTO fact VALUES (1, 1), (2, 2), (3, 3);
-- `k = 1` is repeated and `k = 3` is missing: a join that does not preserve the rows of `fact` changes them.
INSERT INTO dim VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');

-- { echoOn }

-- `ANY` takes at most one row of the other side.
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT ANY JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact LEFT ANY JOIN dim ON fact.k = dim.k ORDER BY id;

SELECT count() FROM (EXPLAIN SELECT name FROM fact RIGHT ANY JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%';
SELECT name FROM fact RIGHT ANY JOIN dim ON fact.k = dim.k ORDER BY name;

-- `ALL` repeats a row for every match,
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact LEFT JOIN dim ON fact.k = dim.k ORDER BY id;

-- unless the other side is unique by the join keys.
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT JOIN (SELECT k, count() AS c FROM dim GROUP BY k) AS d ON fact.k = d.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact LEFT JOIN (SELECT k, count() AS c FROM dim GROUP BY k) AS d ON fact.k = d.k ORDER BY id;

SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT JOIN (SELECT DISTINCT k FROM dim) AS d ON fact.k = d.k) WHERE explain LIKE '%Join (%';
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT JOIN (SELECT k, name FROM dim ORDER BY name LIMIT 1 BY k) AS d ON fact.k = d.k) WHERE explain LIKE '%Join (%';

-- The join keys cover a part of the `GROUP BY` keys only.
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT JOIN (SELECT k, k2 FROM dim GROUP BY k, k2) AS d ON fact.k = d.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact LEFT JOIN (SELECT k, k2 FROM dim GROUP BY k, k2) AS d ON fact.k = d.k ORDER BY id;

-- A view that joins a table whose columns the query does not read.
SELECT count() FROM (EXPLAIN SELECT sum(id) FROM fact_named WHERE id > 1) WHERE explain LIKE '%Join (%';
SELECT sum(id) FROM fact_named WHERE id > 1;

-- A join that only a removed join reads goes away with it.
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT ANY JOIN dim AS d1 ON fact.k = d1.k LEFT ANY JOIN dim AS d2 ON d1.k2 = d2.k) WHERE explain LIKE '%Join (%';

-- A correlated subquery reads the outer table through `CommonSubplanStep`, two of them here, and they go away with their readers.
SELECT count() FROM (EXPLAIN SELECT count() FROM fact LEFT JOIN LATERAL (SELECT count() AS c FROM dim WHERE dim.k < fact.k) AS d ON true) WHERE explain LIKE '%Join (%';
SELECT count() FROM fact LEFT JOIN LATERAL (SELECT count() AS c FROM dim WHERE dim.k < fact.k) AS d ON true SETTINGS correlated_subqueries_use_in_memory_buffer = 1;
SELECT count() FROM fact LEFT JOIN LATERAL (SELECT count() AS c FROM dim WHERE dim.k < fact.k) AS d ON true SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

-- What the join computed of the preserved side stays: the key of `USING` in the common type, and the number of rows when no column is read.
SELECT k, toTypeName(k) FROM fact LEFT ANY JOIN dim USING (k) ORDER BY k;
SELECT count() FROM fact LEFT ANY JOIN dim ON fact.k = dim.k;

-- The join stays if a column of the other side is read,
SELECT count() FROM (EXPLAIN SELECT id, name FROM fact LEFT ANY JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%';

-- if it drops rows of the preserved side,
SELECT count() FROM (EXPLAIN SELECT id FROM fact INNER ANY JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact INNER ANY JOIN dim ON fact.k = dim.k ORDER BY id;

-- as the old `ANY RIGHT` does, which keeps one row of the right side per key,
SELECT count() FROM (EXPLAIN SELECT name FROM fact RIGHT ANY JOIN dim ON fact.k = dim.k SETTINGS any_join_distinct_right_table_keys = 1) WHERE explain LIKE '%Join (%';
SELECT count() FROM fact RIGHT ANY JOIN dim ON fact.k = dim.k SETTINGS any_join_distinct_right_table_keys = 1;

-- if the other side adds to the totals,
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT ANY JOIN (SELECT k, count() AS c FROM dim GROUP BY k WITH TOTALS) AS d ON fact.k = d.k) WHERE explain LIKE '%Join (%';
SELECT id FROM fact LEFT ANY JOIN (SELECT k, count() AS c FROM dim GROUP BY k WITH TOTALS) AS d ON fact.k = d.k ORDER BY id;

-- or if the optimization is disabled.
SELECT count() FROM (EXPLAIN SELECT id FROM fact LEFT ANY JOIN dim ON fact.k = dim.k) WHERE explain LIKE '%Join (%' SETTINGS query_plan_remove_unused_joins = 0;

-- { echoOff }

DROP VIEW fact_named;
DROP TABLE fact;
DROP TABLE dim;
