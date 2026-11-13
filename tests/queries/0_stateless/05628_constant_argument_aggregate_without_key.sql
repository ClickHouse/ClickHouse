-- Without keys, a constant argument of a function that ignores repeated values is added once,
-- instead of being copied to the size of the block.

SET count_distinct_implementation = 'uniqExact', aggregate_functions_null_for_empty = 0;

-- A copy of the constant to 65409 rows would take 6.5 GB.
WITH toFixedString('', 100000) AS c
SELECT uniq(c), uniqExact(c), uniqHLL12(c), length(min(c)), length(max(c)), length(groupUniqArray(c)), count(DISTINCT c),
    COUNTOrDefaultDistinct(c) IGNORE NULLS, length(groupArrayDistinct(c))
FROM numbers(200000) SETTINGS max_block_size = 65409, max_threads = 1, max_memory_usage = 100000000;

-- The constant `c` gives the same result as its full copy `m`.
SELECT 'String', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT 'abc' AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'FixedString', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT toFixedString('ab', 4) AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'Nullable NULL', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT CAST(NULL AS Nullable(String)) AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'Nullable', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT CAST('x' AS Nullable(String)) AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'LowCardinality', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT toLowCardinality('lc') AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'LowCardinality(Nullable)', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT toLowCardinality(CAST('n' AS Nullable(String))) AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'Array', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT [1, 2] AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'Float64', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), sumDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), sumDistinct(m), min(m), max(m))
FROM (SELECT 1.5 AS c, materialize(c) AS m FROM numbers_mt(100000)) SETTINGS max_threads = 4;

SELECT 'String, empty', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT 'abc' AS c, materialize(c) AS m FROM numbers(0));

SELECT 'Nullable NULL, empty', tuple(uniq(c), uniqExact(c), uniqHLL12(c), groupUniqArray(c), COUNTOrDefaultDistinct(c), groupArrayDistinct(c), min(c), max(c)) AS r,
    r <=> tuple(uniq(m), uniqExact(m), uniqHLL12(m), groupUniqArray(m), COUNTOrDefaultDistinct(m), groupArrayDistinct(m), min(m), max(m))
FROM (SELECT CAST(NULL AS Nullable(String)) AS c, materialize(c) AS m FROM numbers(0));

-- `-Distinct` functions add the used values of a `LowCardinality` argument once, in the order of their first occurrence.
SELECT tuple(groupArrayDistinct(x), COUNTOrDefaultDistinct(x), anyDistinct(x)) AS r,
    r <=> tuple(groupArrayDistinct(y), COUNTOrDefaultDistinct(y), anyDistinct(y))
FROM (SELECT toString(number * 7 % 5) AS y, toLowCardinality(y) AS x FROM numbers(1000)) SETTINGS max_threads = 1;

-- A full copy of the `LowCardinality` column would take 6.5 GB.
SELECT COUNTOrDefaultDistinct(x), length(groupArrayDistinct(x)), length(anyDistinct(x))
FROM (SELECT materialize(toLowCardinality(toFixedString('', 100000))) AS x FROM numbers(200000))
SETTINGS max_block_size = 65409, max_threads = 1, max_memory_usage = 100000000;
