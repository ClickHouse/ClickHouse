-- Tags: no-old-analyzer

-- A block nested loop join is counted in the join columns of `system.query_log`, with the kind and
-- strictness it executes.

SET log_queries = 1;
SET allow_block_nested_loop_join = 1;
-- The hash join of the last query is reported as `HASH` whatever the number of threads.
SET join_algorithm = 'hash';
SET any_join_distinct_right_table_keys = 0;
-- A swapped join exchanges the sides, so the logged kind would be the mirrored one. The test
-- harness randomizes the setting.
SET query_plan_join_swap_table = 'false';
-- The `LEFT ANY` query does not read the right table, so its join would be removed.
SET query_plan_remove_unused_joins = 0;

DROP TABLE IF EXISTS bnl_l;
DROP TABLE IF EXISTS bnl_r;

CREATE TABLE bnl_l (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE bnl_r (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO bnl_l SELECT number % 9, number % 7 FROM numbers(40);
INSERT INTO bnl_r SELECT number % 6, number % 5 FROM numbers(30);

-- Every query below runs as a block nested loop join.
SELECT count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl left all';
SELECT count() FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl right all';
SELECT count() FROM bnl_l l FULL JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl full all';
SELECT count() FROM bnl_l l LEFT ANY JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl left any';
SELECT count() FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl left semi';
SELECT count() FROM bnl_l l RIGHT ANTI JOIN bnl_r r ON l.x < r.y FORMAT Null SETTINGS log_comment = 'bnl right anti';

-- One next to a hash join in the same query. Reordering the joins could execute them with their
-- sides swapped, so it is disabled.
SELECT count()
FROM bnl_l l
LEFT JOIN bnl_r r ON l.x < r.y
LEFT JOIN bnl_r r2 ON l.x = r2.x
FORMAT Null SETTINGS log_comment = 'bnl and hash', query_plan_optimize_join_order_limit = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment LIKE 'bnl %'
ORDER BY log_comment;

DROP TABLE bnl_l;
DROP TABLE bnl_r;
