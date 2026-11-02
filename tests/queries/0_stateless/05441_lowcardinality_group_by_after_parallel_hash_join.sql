-- `GROUP BY` on a `LowCardinality` key must not split one key into several groups when the key column reaches the
-- aggregation through a parallel hash join followed by squashing.
--
-- Each probe block has its own small dictionary. The parallel hash join scatters a block into a large piece and a
-- tiny piece that share that dictionary. The large piece is aggregated first, which caches the hashes of the
-- dictionary. The tiny piece is then squashed with pieces of other blocks, which appends their keys to the same
-- dictionary in place. When the squashed chunk is aggregated, the appended keys are past the end of the cached hashes
-- and must be hashed from the key instead of reading past the end, which gave them wrong hashes and duplicate groups.

SELECT count(), uniqExact(toString(k))
FROM
(
    SELECT l.k AS k, count()
    FROM
    (
        SELECT toLowCardinality(toString(intDiv(number, 1000) * 10 + number % 3)) AS k, if(number % 20 = 0, 1, 0) AS j
        FROM numbers(20000)
    ) AS l
    INNER JOIN (SELECT arrayJoin([0, 1]) AS j) AS r ON l.j = r.j
    GROUP BY k
)
SETTINGS max_threads = 2, join_algorithm = 'parallel_hash', max_block_size = 1000,
    min_joined_block_size_rows = 500, min_joined_block_size_bytes = 0, max_joined_block_size_rows = 65409,
    query_plan_join_swap_table = false, max_bytes_before_external_group_by = 0,
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
