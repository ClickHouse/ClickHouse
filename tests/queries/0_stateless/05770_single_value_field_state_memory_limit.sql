-- Tags: no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace `operator new`, so the strings these states keep are not counted against `max_memory_usage`

-- Single-value states that keep large strings in a Field are refused by the memory limit within the block.
SET max_threads = 1, max_block_size = 65536, max_memory_usage = '100M', max_untracked_memory = '1Mi',
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    optimize_aggregators_of_group_by_keys = 0, optimize_aggregation_in_order = 0, preferred_block_size_bytes = 0,
    enable_parallel_replicas = 0;

SELECT sum(c) FROM (SELECT number AS k, sumArgMin(number, tuple(tuple(toFixedString(toString(number % 2), 65535)))) AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.1)) FROM (SELECT number AS k, argMin((repeat(toString(number % 2), 65000), number), number) AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(c) FROM (SELECT number AS k, argMin(number, tuple(repeat(toString(number % 2), 65000))) AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c)) FROM (SELECT number AS k, max(arrayMap(x -> repeat(toString(x), 65000), [number % 2])) AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.1)) FROM (SELECT number % 1024 AS k, max(tuple(repeat('a', if(number < 1024, 1, 65000)))) AS c FROM numbers(2048) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.1)) FROM (SELECT number % 1024 AS k, min(tuple(repeat(if(number < 1024, 'b', 'a'), if(number < 1024, 1, 65000)))) AS c FROM numbers(2048) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c)) FROM (SELECT number AS k, any(repeat(toString(number % 2), 65000)) RESPECT NULLS AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.String)) FROM (SELECT number AS k, anyLast(CAST(repeat(toString(number % 2), 65000), 'Dynamic')) RESPECT NULLS AS c FROM numbers(1024) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.1)) FROM (SELECT number AS k, any(CAST(arrayResize([0.5], 32768), 'QBit(Float32, 32768)')) RESPECT NULLS AS c FROM numbers(512) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }

-- One granule per table, so that every column is read in one block on any storage.
CREATE TABLE t (k UInt64, s AggregateFunction(argMin, Tuple(String, UInt64), UInt64), r AggregateFunction(any_respect_nulls, String),
    m AggregateFunction(max, Tuple(String)), a AggregateFunction(argMin, UInt64, Tuple(String)))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
INSERT INTO t SELECT number, argMinState((repeat(toString(number % 2), 65000), number), number), any_respect_nullsState(repeat(toString(number % 2), 65000)),
    maxState(tuple(repeat(toString(number % 2), 65000))), argMinState(number, tuple(repeat(toString(number % 2), 65000)))
FROM numbers(1000) GROUP BY number SETTINGS max_memory_usage = '2G';
SELECT sum(length(c.1)) FROM (SELECT k, argMinMerge(s) AS c FROM t GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c)) FROM (SELECT k, any_respect_nullsMerge(r) AS c FROM t GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(c.1)) FROM (SELECT k, maxMerge(m) AS c FROM t GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(c) FROM (SELECT k, argMinMerge(a) AS c FROM t GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
DROP TABLE t;

-- A column of 2000 such states is over the limit while it is read.
CREATE TABLE t2 (k UInt64, s AggregateFunction(argMin, Tuple(String, UInt64), UInt64), r AggregateFunction(any_respect_nulls, String))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
INSERT INTO t2 SELECT number, argMinState((repeat(toString(number % 2), 65000), number), number), any_respect_nullsState(repeat(toString(number % 2), 65000))
FROM numbers(2000) GROUP BY number SETTINGS max_memory_usage = '2G';
SELECT s FROM t2 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT r FROM t2 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
DROP TABLE t2;

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(memory_usage <= 110000000) FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'ExceptionWhileProcessing' AND exception_code = 241;

SELECT argMin((toString(number), number), number), max([toString(number)]), any(toString(number)) RESPECT NULLS,
    sumArgMin(number, tuple(toString(number % 3))), length(tupleElement(argMax((repeat('a', 2000), number), number), 1)),
    any(CAST([1, 2], 'QBit(Float32, 2)'))
FROM numbers(100);
