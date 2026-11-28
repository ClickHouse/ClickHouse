SET optimize_redundant_comparisons = 1;

CREATE TABLE t (s String) ENGINE = Memory;
INSERT INTO t SELECT toString(number % 10) FROM numbers(10);

-- A comparison implied by a later one is removed; the later one must take its place, ahead of the conditions it guards.
SELECT count() FROM t WHERE toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) > 5;
SELECT count() FROM t WHERE toUInt64(s) != 9 AND intDiv(10, 9 - toUInt64(s)) > 0 AND toUInt64(s) < 5;
SELECT count() FROM t WHERE toUInt64(s) > 1 AND intDiv(10, toUInt64(s) - 1) > 0 AND toUInt64(s) >= 3;
SELECT count() FROM t WHERE toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) = 5;
SELECT count() FROM t WHERE toUInt64(s) != 9 AND intDiv(10, 9 - toUInt64(s)) > 0 AND toUInt64(s) <= 9;
SELECT count() FROM t WHERE toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) > 3 AND intDiv(10, toUInt64(s) - 3) > 0 AND toUInt64(s) > 5;
SELECT count() FROM t WHERE s != '' AND toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) > 5;
SELECT countIf(toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) > 5) FROM t;

-- Pruning still happens: the surviving comparison is the first argument of the rewritten AND.
SELECT count() FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(10) WHERE number != 0 AND intDiv(10, number) > 0 AND number = 5) WHERE explain ILIKE '%WHERE and(equals(__table1.number, 5), greater(intDiv(%';
SELECT count() FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(10) WHERE number != 9 AND intDiv(10, 9 - number) > 0 AND number <= 9) WHERE explain ILIKE '%WHERE and(less(__table1.number, 9), greater(intDiv(%';
SELECT count() FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(10) WHERE number != 3 AND intDiv(10, number - 3) > 0 AND number >= 3) WHERE explain ILIKE '%WHERE and(greater(__table1.number, 3), greater(intDiv(%';
SELECT count() FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(10) WHERE number != 0 AND intDiv(10, number) > 0 AND number > 3 AND intDiv(10, number - 3) > 0 AND number > 5) WHERE explain ILIKE '%WHERE and(greater(__table1.number, 5), greater(intDiv(10, __table1.number), 0), greater(intDiv(%';

CREATE TABLE t_mt (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mt SELECT toString(number % 10) FROM numbers(10);
SELECT count() FROM t_mt PREWHERE toUInt64(s) != 0 AND intDiv(10, toUInt64(s)) > 0 AND toUInt64(s) > 5;

DROP TABLE t;
DROP TABLE t_mt;
