-- A state that serialize or merge reads must not change: several threads can read one shared state at once.

-- One shared state serialized as a GROUP BY key by several threads.
SELECT count()
FROM
(
    SELECT k
    FROM
    (
        SELECT number, (SELECT estimateCompressionRatioState(number) FROM numbers(5000)) AS k
        FROM numbers_mt(200000)
    )
    GROUP BY k
)
SETTINGS max_threads = 16, max_block_size = 1000, optimize_group_by_constant_keys = 1;

-- One shared state merged by several threads.
SELECT estimateCompressionRatioMerge('LZ4', 1024)(k) = (SELECT estimateCompressionRatio('LZ4', 1024)(number) FROM numbers(1000))
FROM
(
    SELECT (SELECT estimateCompressionRatioState('LZ4', 1024)(number) FROM numbers(1000)) AS k
    FROM numbers_mt(10000)
)
SETTINGS max_threads = 16, max_block_size = 100;

-- A window state is read on every row while it still receives rows: the last row covers the whole frame.
SELECT finalizeAggregation(s) = (SELECT estimateCompressionRatio('LZ4', 1024)(number) FROM numbers(1000))
FROM
(
    SELECT number, estimateCompressionRatioState('LZ4', 1024)(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
    FROM numbers(1000)
)
ORDER BY number DESC
LIMIT 1;

-- A serialized state that is still open keeps its flushed blocks and its open block.
SELECT finalizeAggregation(CAST(CAST(s AS String) AS AggregateFunction(estimateCompressionRatio('LZ4', 1024), UInt64)))
    = (SELECT estimateCompressionRatio('LZ4', 1024)(number) FROM numbers(1000))
FROM (SELECT estimateCompressionRatioState('LZ4', 1024)(number) AS s FROM numbers(1000));
