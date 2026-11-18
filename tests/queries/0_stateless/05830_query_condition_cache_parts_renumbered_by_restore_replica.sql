-- Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree
-- no-replicated-database, no-ordinary-database, no-shared-merge-tree: `ATTACH ... AS REPLICATED` needs a MergeTree table in an Atomic database.

-- `SYSTEM RESTORE REPLICA` attaches the parts again with block numbers from the new Keeper counters, so the part with 'apple'
-- becomes `all_1_1_0`, the name the part with 'pear' had. The query condition cache must not return the verdicts of that part.

SET use_query_condition_cache = 1;
-- The uncompressed cache is keyed by file path, so it can serve the other part's data here too.
SET use_uncompressed_cache = 0;

CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t;
INSERT INTO t VALUES (1, 'pear');
INSERT INTO t VALUES (2, 'apple');
SELECT sum(k) FROM t WHERE length(s) = 5;
DETACH TABLE t;
ATTACH TABLE t AS REPLICATED;
SYSTEM STOP MERGES t;
SYSTEM RESTORE REPLICA t;
SELECT _part, k FROM t ORDER BY _part;
SELECT sum(k) FROM t WHERE length(s) = 5;

-- The same with a part that has more marks than the one whose name it takes.
CREATE TABLE t2 (k UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t2;
INSERT INTO t2 SELECT number, 'pear' FROM numbers(16);
INSERT INTO t2 SELECT number, 'apple' FROM numbers(1000);
SELECT sum(k) FROM t2 WHERE length(s) = 5;
DETACH TABLE t2;
ATTACH TABLE t2 AS REPLICATED;
SYSTEM STOP MERGES t2;
SYSTEM RESTORE REPLICA t2;
SELECT _part, count() FROM t2 GROUP BY _part ORDER BY _part;
SELECT sum(k) FROM t2 WHERE length(s) = 5;
