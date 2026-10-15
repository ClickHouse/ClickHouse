-- Tags: no-fasttest, no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: without jemalloc, the memory tracker does not see `malloc`, which `groupBitmap` uses.
-- Every row of a window over the whole partition gets its own copy of the aggregate state. uniqTheta allocates it
-- with operator new and groupBitmap with malloc, which the memory tracker does not refuse: the window must check the limit itself.

SELECT uniqThetaState(number) OVER () FROM numbers(3800) SETTINGS max_memory_usage = '180M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- A plain aggregate function whose result is a state copies it per row too.
SELECT any(s) OVER () FROM (SELECT (SELECT uniqThetaState(number) FROM numbers(3800)) AS s FROM numbers(3800)) SETTINGS max_memory_usage = '180M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- So does a Dynamic value holding a state.
SELECT any(CAST(s AS Dynamic)) OVER () FROM (SELECT (SELECT uniqThetaState(number) FROM numbers(3800)) AS s FROM numbers(3800)) SETTINGS max_memory_usage = '180M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- A few rows of large states: the limit is checked within the block, not every N rows.
SELECT any(s) OVER () FROM (SELECT (SELECT groupBitmapState(number * 2) FROM numbers(10000000)) AS s FROM numbers(60)) SETTINGS max_memory_usage = '40M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Taking ownership of a column that refers to one state many times copies them all in one loop (leadInFrame does it at the end of a block).
SELECT finalizeAggregation(materialize(s)) FROM (SELECT (SELECT uniqThetaState(number) FROM numbers(3800)) AS s FROM numbers(3800)) SETTINGS max_memory_usage = '180M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT count(), uniqThetaMerge(s) FROM (SELECT uniqThetaState(number) OVER () AS s FROM numbers(3800)) SETTINGS max_memory_usage = '1G';
