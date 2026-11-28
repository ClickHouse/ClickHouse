-- Tags: distributed
-- Tag distributed: the shipped plan needs the predefined `test_shard_localhost` cluster.

-- `TotalsHavingStep::serialize` and `::deserialize`, src/Processors/QueryPlan/TotalsHavingStep.cpp.
-- A single-shard `Distributed` read answers `Complete` as its processing stage, so at
-- `serialize_query_plan = 1` the whole plan - the `TotalsHaving` step of `GROUP BY .. WITH TOTALS`
-- included - is serialized on the initiator and rebuilt on the shard out of the serialized flags
-- (final, overflow_row, the optional HAVING `ActionsDAG` with its filter column and remove_filter)
-- and the two serialized settings `totals_mode` and `totals_auto_threshold`.
--
-- Asserted behaviour, docs/en/sql-reference/statements/select/group-by.md ("WITH TOTALS Modifier"):
-- with `totals_mode = 'after_having_exclusive'` the totals row counts only the groups that passed
-- HAVING, with `before_having` it counts every group. Every shipped read below is repeated at
-- `serialize_query_plan = 0` and against the local table, so each value is an oracle rather than a
-- snapshot: the round trip must not change the groups or the totals row.

DROP TABLE IF EXISTS t_05513;
DROP TABLE IF EXISTS t_05513_dist;

CREATE TABLE t_05513 (g UInt8, v UInt32) ENGINE = MergeTree ORDER BY g;
-- Group sums: 1 -> 50, 2 -> 70, 3 -> 90, and 210 over all three.
INSERT INTO t_05513 SELECT number % 3 + 1, (number + 1) * 10 FROM numbers(6);
CREATE TABLE t_05513_dist AS t_05513 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05513);

SET prefer_localhost_replica = 0;
-- The aggregation has to stay inside the shipped plan: the parallel-replicas job enables parallel
-- replicas in the profile and `automatic_parallel_replicas_mode` is randomized.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
-- The two settings the step writes out and reads back.
SET totals_mode = 'after_having_exclusive', totals_auto_threshold = 0.5;

SELECT '-- HAVING: groups 70 and 90, totals 160 because the dropped group is excluded';
SELECT g, sum(v) FROM t_05513 GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g
    SETTINGS serialize_query_plan = 1;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g
    SETTINGS serialize_query_plan = 0;

SELECT '-- no HAVING, so the step carries no ActionsDAG: three groups, totals 210';
SELECT g, sum(v) FROM t_05513 GROUP BY g WITH TOTALS ORDER BY g;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS ORDER BY g
    SETTINGS serialize_query_plan = 1;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS ORDER BY g
    SETTINGS serialize_query_plan = 0;

SELECT '-- totals_mode = before_having: the same two groups, but totals 210';
SELECT g, sum(v) FROM t_05513 GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g
    SETTINGS totals_mode = 'before_having';
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g
    SETTINGS totals_mode = 'before_having', serialize_query_plan = 1;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH TOTALS HAVING sum(v) > 60 ORDER BY g
    SETTINGS totals_mode = 'before_having', serialize_query_plan = 0;

-- WITH ROLLUP leaves the finalization to the rollup step, so the serialized `final` flag is false
-- here and true in every case above. The `g = 0` data row is the rollup subtotal, the one after the
-- blank line is the totals row.
SELECT '-- WITH ROLLUP WITH TOTALS: the step is not final';
SELECT g, sum(v) FROM t_05513 GROUP BY g WITH ROLLUP WITH TOTALS ORDER BY g;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH ROLLUP WITH TOTALS ORDER BY g
    SETTINGS serialize_query_plan = 1;
SELECT g, sum(v) FROM t_05513_dist GROUP BY g WITH ROLLUP WITH TOTALS ORDER BY g
    SETTINGS serialize_query_plan = 0;

DROP TABLE t_05513_dist;
DROP TABLE t_05513;
