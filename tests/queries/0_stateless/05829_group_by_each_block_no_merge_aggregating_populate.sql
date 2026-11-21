-- `group_by_each_block_no_merge` with an aggregate function state that has a nontrivial destructor
-- (`uniqExactState`). Every emitted state is owned by `ColumnAggregateFunction` while the aggregation state
-- of the transform is reset for the next block, so it must survive the insertion into `AggregatingMergeTree`
-- and the merges that follow, without being destroyed twice or leaked.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

-- A `Memory` table keeps the inserted blocks of 10 rows, so the reads below see 100 blocks.
CREATE TABLE src (k UInt64, v String) ENGINE = Memory;
INSERT INTO src SELECT number % 7, toString(number % 23) FROM numbers(1000)
SETTINGS max_block_size = 10, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

-- Each of the 100 input blocks of 10 rows emits a partial state for 7 keys, so there are more rows than keys.
SELECT count() > 7
FROM (SELECT k, uniqExactState(v) FROM src GROUP BY k)
SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1;

-- The partial states are kept as separate rows of one part: `optimize_on_insert` is disabled and merges are
-- stopped.
CREATE TABLE dst (k UInt64, s AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY k;
SYSTEM STOP MERGES dst;
INSERT INTO dst SELECT k, uniqExactState(v) FROM src GROUP BY k
SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1, optimize_on_insert = 0;

SELECT count() > 7 FROM dst;
SELECT k, uniqExactMerge(s) FROM dst GROUP BY k ORDER BY k;

SYSTEM START MERGES dst;
OPTIMIZE TABLE dst FINAL;
SELECT count() FROM dst;
SELECT k, finalizeAggregation(s) FROM dst ORDER BY k;

-- The documented use case: a materialized view populated with partial states that are merged later.
SET group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1;
CREATE MATERIALIZED VIEW mv ENGINE = AggregatingMergeTree ORDER BY k POPULATE
AS SELECT k, uniqExactState(v) AS s FROM src GROUP BY k;
SET group_by_each_block_no_merge = 0;

SELECT k, uniqExactMerge(s) FROM mv GROUP BY k ORDER BY k;

-- The same as an exact aggregation over the source.
SELECT k, uniqExact(v) FROM src GROUP BY k ORDER BY k;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
