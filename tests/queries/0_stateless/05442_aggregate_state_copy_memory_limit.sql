-- Tags: no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace `operator new`, so the copies are not counted against `max_memory_usage`

-- Copying a state of `anyLast` or `argMax` over a `FixedString` allocates its `Field` with `operator new`, which the
-- memory tracker does not refuse, so the code that copies states checks the limit itself.

-- `materialize` repeats one state, and `finalizeAggregation` makes the column own all of them.
SELECT finalizeAggregation(materialize(initializeAggregation('argMaxState', 1, toFixedString('\0', 1048577)))) FROM numbers(300) SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- With an explicit default, every row outside the frame gets its own copy of the default state.
SELECT lagInFrame(s, 1000, s) OVER () FROM (SELECT initializeAggregation('anyLastState', toNullable(toFixedString('\0', 1048577))) AS s FROM numbers(300)) SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SET allow_deprecated_error_prone_window_functions = 1;

-- The out-of-range rows make the column take ownership of all states it shares with the constant source.
SELECT neighbor(initializeAggregation('anyLastState', toNullable(toFixedString('\0', 1048577))), 10) FROM numbers(300) SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- With a negative offset the defaults come first, and every in-range row is copied as it is inserted.
SELECT neighbor(initializeAggregation('anyLastState', toNullable(toFixedString('\0', 1048577))), -10) FROM numbers(300) SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT countIf(isNull(finalizeAggregation(a))), countIf(isNull(finalizeAggregation(b))), countIf(finalizeAggregation(a) = 'ab'), count()
FROM (SELECT neighbor(s, 10) AS a, neighbor(s, -10) AS b FROM (SELECT initializeAggregation('anyLastState', toNullable(toFixedString('ab', 2))) AS s FROM numbers(300)))
SETTINGS max_block_size = 65536;
