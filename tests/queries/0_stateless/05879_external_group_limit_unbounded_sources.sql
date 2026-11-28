-- A spilled `DISTINCT` or `LIMIT BY` returns its remaining rows only at the end of its input, so over a
-- source that never ends by itself both stay in memory, and an outer `LIMIT` ends the query. A 1-byte
-- threshold would make them spill from their first chunk. If one spilled, `max_execution_time` would break
-- its query before it returns a row.
SET max_threads = 1;
SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1;
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 1;
SET max_execution_time = 20, timeout_overflow_mode = 'break';

-- The full scan of a table without a size, for both operators.
SELECT number FROM system.numbers LIMIT 1 BY number % 10 LIMIT 3;
SELECT DISTINCT number % 10 FROM system.numbers LIMIT 3;

-- A filter without an upper bound reads up to the end of the `UInt64` domain. The read of `numbers` checks
-- this size against `max_rows_to_read` before it starts, so it lifts the limit that the test profile sets.
SELECT number FROM numbers() WHERE number > 5 LIMIT 1 BY number % 10 LIMIT 3 SETTINGS max_rows_to_read = 0;
SELECT prime FROM primes() WHERE prime > 100 LIMIT 1 BY prime % 10 LIMIT 3;

-- The other sources that never end. Random data shows only the number of rows.
SELECT prime FROM primes() LIMIT 1 BY prime % 10 LIMIT 3;
SELECT DISTINCT zero FROM system.zeros LIMIT 1;
SELECT number FROM loop(numbers(3)) LIMIT 1 BY number LIMIT 3;
SELECT count() FROM (SELECT DISTINCT * FROM generateRandom('x UInt8', 1) LIMIT 3);
SELECT count() FROM (SELECT DISTINCT * FROM fuzzJSON('{"a": 1}', 1) LIMIT 3);
-- The query fuzzer generates rows slowly, so it generates small blocks.
SELECT count() FROM (SELECT DISTINCT * FROM fuzzQuery('SELECT 1', 500, 1) LIMIT 3) SETTINGS max_block_size = 16;
SELECT count() FROM (SELECT DISTINCT * FROM obfuscate(SELECT number FROM numbers(100)) LIMIT 3);
-- A recursive CTE ends only after a step that adds no rows, and every step below adds one.
WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t)
SELECT n FROM t LIMIT 1 BY n LIMIT 3;

-- A union never ends while one of its inputs never ends, and neither does a merge join, which reads both
-- sides together.
SELECT count() FROM
(
    SELECT DISTINCT x FROM (SELECT number % 10 AS x FROM numbers(10) UNION ALL SELECT number % 10 FROM system.numbers)
    LIMIT 3
);
SELECT countIf(explain LIKE '%ExternalDistinctTransform%') = 0 FROM
(
    EXPLAIN PIPELINE SELECT DISTINCT n.number FROM numbers(10) AS n JOIN generateRandom('x UInt64', 1) AS r ON n.number = r.x
    SETTINGS join_algorithm = 'full_sorting_merge'
);

-- An endless source that a `LIMIT` ends, or that only feeds the build side of a hash join, leaves the input
-- of an operator bounded, so the operator spills as over a table.
SELECT countIf(explain LIKE '%ExternalDistinctTransform%') > 0 FROM
(
    EXPLAIN PIPELINE SELECT DISTINCT x FROM (SELECT x FROM generateRandom('x UInt8', 1) LIMIT 1000)
);
SELECT countIf(explain LIKE '%ExternalLimitByTransform%') > 0 FROM
(
    EXPLAIN PIPELINE SELECT x FROM
    (
        SELECT number AS x FROM numbers(10)
        UNION ALL SELECT x FROM (SELECT x FROM generateRandom('x UInt64', 1) LIMIT 10)
    )
    LIMIT 1 BY x
);
SELECT countIf(explain LIKE '%ExternalDistinctTransform%') > 0 FROM
(
    EXPLAIN PIPELINE SELECT DISTINCT n.number FROM numbers(10) AS n JOIN generateRandom('x UInt64', 1) AS r ON n.number = r.x
    SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0
);
