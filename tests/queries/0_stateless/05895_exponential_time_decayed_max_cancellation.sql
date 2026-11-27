-- `exponentialTimeDecayedMax` scans the whole frame for every row: one block of a million rows over a
-- whole-partition frame does not finish within the test time limit unless the deadline is checked inside the block.
SELECT exponentialTimeDecayedMax(1)(number, number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(1000000)
SETTINGS max_execution_time = 1, max_block_size = 1000000
FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }
