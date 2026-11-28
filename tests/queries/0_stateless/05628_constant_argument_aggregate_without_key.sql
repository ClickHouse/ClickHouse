-- Without keys, a constant argument is not copied to the size of the block: a function that ignores
-- repeated values adds it once, any other function adds a copy of at most 1 MiB repeatedly.

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

-- Functions that do not ignore repeated values: a copy of the constant to 5000 rows would take 500 MB.
WITH toFixedString('', 100000) AS c
SELECT count(c), length(any(c)), length(anyLast(c)), length(anyOrDefault(c)), length(argMax(c, c)), length(anyHeavy(c)), countIf(c, 1 = 1)
FROM numbers(5000) SETTINGS max_block_size = 5000, max_threads = 1, max_memory_usage = 100000000, optimize_normalize_count_variants = 0;

-- A constant of 1 MiB is not folded by the analyzer, so `count` keeps its argument: 64 GiB for a copy to 65409 rows.
SELECT count(toFixedString('', 1048576)) FROM numbers(200000) SETTINGS max_block_size = 65409, max_threads = 1, max_memory_usage = 100000000;
SELECT length(anyOrDefault(toFixedString(CAST('' AS LowCardinality(Nullable(FixedString(12)))), 1048576))) FROM numbers(200000)
SETTINGS max_block_size = 65409, max_threads = 1, max_memory_usage = 100000000;

-- A row of the constant takes 1 MB without `LowCardinality`: 5 GB for a copy to 5000 rows.
SELECT count(c), length(any(c))
FROM (SELECT CAST(arrayWithConstant(1000, repeat('x', 1000)) AS Array(LowCardinality(String))) AS c FROM numbers(5000))
SETTINGS max_block_size = 5000, max_threads = 1, max_memory_usage = 100000000, optimize_normalize_count_variants = 0;

-- A constant of about 1 KB is added in five copies of about 1000 rows, the last one partial.
SELECT 'String, copies', tuple(count(c), length(any(c)), length(anyLast(c)), length(anyOrDefault(c)), length(argMax(c, c)), length(groupArray(c)),
    length(groupConcat(c)), countIf(c, 1 = 1), length(topK(1)(c))) AS r,
    r <=> tuple(count(m), length(any(m)), length(anyLast(m)), length(anyOrDefault(m)), length(argMax(m, m)), length(groupArray(m)),
    length(groupConcat(m)), countIf(m, 1 = 1), length(topK(1)(m)))
FROM (SELECT repeat('x', 1000) AS c, materialize(c) AS m FROM numbers(5000))
SETTINGS max_block_size = 5000, max_threads = 1, optimize_normalize_count_variants = 0;

SELECT 'Numeric, copies', tuple(sum(c), avg(c), count(c), sumArray(a), count(a), finalizeAggregation(sumState(c)), sumIf(c, 1 = 1), uniq(c, a)) AS r,
    r <=> tuple(sum(mc), avg(mc), count(mc), sumArray(ma), count(ma), finalizeAggregation(sumState(mc)), sumIf(mc, 1 = 1), uniq(mc, ma))
FROM (SELECT 1.5 AS c, [1, 2, 3] AS a, materialize(c) AS mc, materialize(a) AS ma FROM numbers(300000))
SETTINGS max_block_size = 300000, max_threads = 1, optimize_normalize_count_variants = 0;

SELECT 'Nullable, copies', tuple(count(c), any(c), count(n), any(n), sum(n)) AS r,
    r <=> tuple(count(mc), any(mc), count(mn), any(mn), sum(mn))
FROM (SELECT CAST('x' AS Nullable(String)) AS c, CAST(NULL AS Nullable(Float64)) AS n, materialize(c) AS mc, materialize(n) AS mn FROM numbers(5000))
SETTINGS max_block_size = 5000, max_threads = 1, optimize_normalize_count_variants = 0;

SELECT 'String, copies, empty', tuple(count(c), length(any(c)), length(anyLast(c)), length(anyOrDefault(c)), length(argMax(c, c)), length(groupArray(c)),
    length(groupConcat(c)), countIf(c, 1 = 1), length(topK(1)(c))) AS r,
    r <=> tuple(count(m), length(any(m)), length(anyLast(m)), length(anyOrDefault(m)), length(argMax(m, m)), length(groupArray(m)),
    length(groupConcat(m)), countIf(m, 1 = 1), length(topK(1)(m)))
FROM (SELECT repeat('x', 1000) AS c, materialize(c) AS m FROM numbers(0))
SETTINGS max_threads = 1, optimize_normalize_count_variants = 0;
