-- `addBatchArray` (used by `arrayReduce` and the `-Array` combinator) sends a row with a long array
-- to `addBatchSinglePlace` and adds a short one element by element (always for integer `sum`/`avg` and
-- `count`). Mix both kinds of rows within and across blocks and compare with results computed another way.

DROP TABLE IF EXISTS t_add_batch_array;

CREATE TABLE t_add_batch_array (id UInt32, k UInt32, a Array(Int64), n Array(Nullable(Int64)), c Array(UInt8), f Array(Float64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_add_batch_array
SELECT
    number AS id,
    number % 10 AS k,
    arrayMap(i -> toInt64((number * 31 + i * 7) % 13) - 6, range([0, 1, 2, 3, 17, 100, 1, 0, 40][number % 9 + 1])) AS a,
    arrayMap(x -> if(x % 3 = 0, NULL, x), a) AS n,
    arrayMap(x -> toUInt8(x > 0), a) AS c,
    arrayMap(x -> x / 7, a) AS f
FROM numbers(1000);

SET max_block_size = 7;

SELECT
    count(),
    countIf(arrayReduce('sum', a) != arraySum(a)),
    countIf(arrayReduce('count', a) != length(a)),
    countIf(ifNull(arrayReduce('sum', n), 0) != arraySum(arrayMap(x -> ifNull(x, 0), n))),
    countIf(arrayReduce('count', n) != length(arrayFilter(x -> x IS NOT NULL, n))),
    countIf(arrayReduce('sumIf', a, c) != arraySum(arrayFilter((x, f) -> f, a, c))),
    countIf(arrayReduce('uniqExact', a) != length(arrayDistinct(a))),
    countIf(arrayReduce('sumDistinct', a) != arraySum(arrayDistinct(a))),
    countIf(arrayReduce('max', a) != arrayMax(a) AND notEmpty(a)),
    countIf(arrayReduce('groupArray', a) != a),
    countIf(finalizeAggregation(arrayReduce('sumState', a)) != arraySum(a)),
    countIf(arrayReduce('sumForEach', [a, a]) != arrayMap(x -> 2 * x, a)),
    countIf(abs(arrayReduce('sum', f) - arraySum(f)) > 1e-9),
    countIf(abs(arrayReduce('avg', f) - arrayAvg(f)) > 1e-9 AND notEmpty(f))
FROM t_add_batch_array;

SELECT sum(arrayReduce('sum', a)), sum(arrayReduce('sumIf', a, c)), sum(ifNull(arrayReduce('sum', n), 0)) FROM t_add_batch_array;

-- Mostly empty arrays, so most blocks have no row long enough for `addBatchSinglePlace`.
SELECT
    countIf(ifNull(arrayReduce('sum', s), 0) != arraySum(arrayMap(x -> ifNull(x, 0), s))),
    countIf(arrayReduce('max', s) IS DISTINCT FROM arrayMax(s) AND notEmpty(arrayFilter(x -> x IS NOT NULL, s))),
    sum(ifNull(arrayReduce('sum', s), 0))
FROM (SELECT arrayMap(i -> if(i % 3 = 0, NULL, toInt64(number % 7 + i)), range(if(number % 50 = 0, 20, 0))) AS s FROM numbers(1000));

-- The `-Array` combinator in `GROUP BY`, against the same aggregates over `ARRAY JOIN`.
SELECT k, sumArray(a), maxArray(a), ifNull(sumArray(n), 0), uniqExactArray(a), sumIfArray(a, c), arraySort(groupArrayArray(a)) FROM t_add_batch_array WHERE notEmpty(a) GROUP BY k
EXCEPT
SELECT k, sum(x), max(x), ifNull(sum(y), 0), uniqExact(x), sumIf(x, cf), arraySort(groupArray(x)) FROM t_add_batch_array ARRAY JOIN a AS x, n AS y, c AS cf GROUP BY k;

-- The batch loop of a float `sum` may round differently, so the result for a row must not depend on the other rows of its block.
SELECT
    (SELECT sum(cityHash64(number, arrayReduce('sum', s))) FROM (SELECT number, arrayMap(i -> 0.1, range(if(number % 200 = 0, 100, 0))) AS s FROM numbers(1000)) SETTINGS max_block_size = 8)
    = (SELECT sum(cityHash64(number, arrayReduce('sum', s))) FROM (SELECT number, arrayMap(i -> 0.1, range(if(number % 200 = 0, 100, 0))) AS s FROM numbers(1000)) SETTINGS max_block_size = 200);

DROP TABLE t_add_batch_array;
