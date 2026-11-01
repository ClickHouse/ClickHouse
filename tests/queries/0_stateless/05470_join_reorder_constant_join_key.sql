-- Join reordering must not join two tables only through a constant join key: with transitive predicates,
-- `w.src = p.src` and `r.src = p.src` imply `w.src = r.src`, which holds for every pair of rows.

SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE tp (id UInt64, k String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tw (src String, k String, brand String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tc (src String, id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO tp SELECT number % 1000, 'k' || toString(number % 1000) FROM numbers(100000);
INSERT INTO tw SELECT 'wlr', 'k' || toString(number), 'b' FROM numbers(1000);
INSERT INTO tc SELECT 'wlr', number, number FROM numbers(1000);

CREATE VIEW p AS SELECT materialize('wlr') AS src, id, k FROM tp;
CREATE VIEW w AS SELECT materialize('wlr') AS src, k, brand FROM tw;
CREATE VIEW r AS SELECT materialize('wlr') AS src, id, x FROM tc;
CREATE VIEW p_union AS SELECT materialize('wlr') AS src, id, k FROM tp WHERE id < 500
    UNION ALL SELECT materialize('wlr'), id, k FROM tp WHERE id >= 500;
CREATE VIEW p_union_mixed AS SELECT materialize('wlr') AS src, id, k FROM tp WHERE id < 500
    UNION ALL SELECT materialize('xyz'), id, k FROM tp WHERE id >= 500;
CREATE VIEW p_function AS SELECT reverse(materialize('rlw')) AS src, id, k FROM tp;
CREATE VIEW p_function_of_column AS SELECT reverse(toString(id)) AS src, id, k FROM tp;
CREATE VIEW p_limit_by AS SELECT materialize('wlr') AS src, id, k FROM tp LIMIT 100 BY id;

-- Each query counts the joins whose only condition is the constant key.
SELECT 'default', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id)
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'greedy', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy')
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'dpsize', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize')
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'dphyp', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp')
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'dpsub', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub')
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'randomized estimates', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'randomized estimates', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 8)
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

SELECT 'randomized estimates', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 16)
WHERE explain ILIKE '%Join conditions: materialize(\'wlr\') = materialize(\'wlr\')';

-- Only the middle relation has the constant key, so `w.src = r.src` would join two stored columns on it.
-- There the constant comes from every branch of a UNION ALL.
SELECT 'union', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_union AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: src = src';

SELECT 'union', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_union AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 17)
WHERE explain ILIKE '%Join conditions: src = src';

-- Branches with different constants give no constant column, so `w.src = r.src` is still inferred and used.
SELECT 'union, different constants', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_union_mixed AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: src = src';

-- There it is a deterministic function of a constant.
SELECT 'function', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_function AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: src = src';

-- A function of a column gives no constant column, so `w.src = r.src` is still inferred and used.
SELECT 'function of a column', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_function_of_column AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: src = src';

-- There it passes through LIMIT BY.
SELECT 'limit by', count() FROM (EXPLAIN PLAN
    SELECT count() FROM p_limit_by AS p JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 3)
WHERE explain ILIKE '%Join conditions: src = src';

-- There it comes from the preserved side of a join that enters the join graph as one relation.
SELECT 'sub-join', count() FROM (EXPLAIN PLAN
    SELECT count() FROM (SELECT p.src AS src, p.id AS id, p.k AS k FROM p ANY LEFT JOIN tc ON p.id = tc.id) AS p
    JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS use_statistics = 0)
WHERE explain ILIKE '%Join conditions: src = src';

SELECT 'sub-join', count() FROM (EXPLAIN PLAN
    SELECT count() FROM (SELECT p.src AS src, p.id AS id, p.k AS k FROM p ANY LEFT JOIN tc ON p.id = tc.id) AS p
    JOIN tw AS w ON w.src = p.src AND w.k = p.k JOIN tc AS r ON r.src = p.src AND r.id = p.id
    SETTINGS query_plan_optimize_join_order_randomize = 14)
WHERE explain ILIKE '%Join conditions: src = src';

SELECT count() FROM p JOIN w ON w.src = p.src AND w.k = p.k JOIN r ON r.src = p.src AND r.id = p.id;

DROP VIEW p_limit_by;
DROP VIEW p_function_of_column;
DROP VIEW p_function;
DROP VIEW p_union_mixed;
DROP VIEW p_union;
DROP VIEW r;
DROP VIEW w;
DROP VIEW p;
DROP TABLE tc;
DROP TABLE tw;
DROP TABLE tp;
