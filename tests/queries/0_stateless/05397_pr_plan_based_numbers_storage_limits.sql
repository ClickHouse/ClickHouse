-- A `numbers` read shipped to a replica is rebuilt there from the serialized plan, so it has to get the read
-- limits of the query back: without them a byte or leaf limit set by the user is not enforced on the replica.
-- `numbers` checks the row limits itself when the read is built, so a byte limit is what exercises the limits
-- attached to the source.

DROP TABLE IF EXISTS t_numbers_limits SYNC;

CREATE TABLE t_numbers_limits (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_numbers_limits SELECT number FROM numbers(10);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1,
    -- Only remote replicas run the fragment, so the `numbers` read is a deserialized one.
    parallel_replicas_local_plan = 0;
-- The `MergeTree` table is the build side and is read first, so its source does not see the rows of `numbers`.
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0;

-- 100000 numbers are 800000 bytes; the leaf limit is checked only by the sources on the replica.
SELECT count() FROM numbers(100000) AS l RIGHT JOIN t_numbers_limits AS r ON l.number = r.key
SETTINGS max_bytes_to_read_leaf = 100000, read_overflow_mode_leaf = 'throw'; -- { serverError TOO_MANY_BYTES }

SELECT count() FROM numbers(100000) AS l RIGHT JOIN t_numbers_limits AS r ON l.number = r.key;

DROP TABLE t_numbers_limits SYNC;
