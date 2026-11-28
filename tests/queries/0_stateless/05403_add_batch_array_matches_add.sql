-- For these functions `addBatchSinglePlace` differs from `add` (another of equal values than the first, NaN
-- and NULL nested in a value ordered differently, a condition of 2), so `arrayReduce` of an array of at
-- least 16 elements must still give what `add` gives. With one row per block, the array is the whole column.

SET max_block_size = 1;

SELECT arrayReduce('argMin', range(20), arrayMap(x -> 'z', range(20))), arrayReduce('argMax', range(20), arrayMap(x -> 'z', range(20)));
SELECT arrayReduce('argMax', range(30), arrayMap(x -> [1], range(30)));
SELECT arrayReduce('max', arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(20)));
SELECT arrayReduce('min', arrayMap(x -> [if(x = 0, NULL, x)], range(20)));
SELECT arrayReduce('max', arrayMap(x -> tuple(if(x = 0, nan, toFloat64(x))), range(20)));
SELECT groupArray(r) FROM (SELECT arrayReduce('argMin', range(20), arrayMap(x -> 'z', range(20))) AS r FROM numbers(3));

-- A condition other than 0 or 1 selects the row, as in `add`.
SELECT timeSeriesGroupArrayIf([toDateTime(1700000000 + number, 'UTC')], [toFloat64(number)], toUInt8(2)) FROM numbers(3)
SETTINGS enable_time_series_aggregate_functions = 1;

-- `-Distinct` passes the new values to `addBatchSinglePlace` of the nested function, so it must add the elements
-- one by one as well, below and above the 16 elements of the batch path, also inside other combinators.
SELECT arrayReduce('maxDistinct', arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(10))), arrayReduce('maxDistinct', arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(20)));
SELECT arrayReduce('minDistinct', arrayMap(x -> [if(x = 0, NULL, x)], range(10))), arrayReduce('minDistinct', arrayMap(x -> [if(x = 0, NULL, x)], range(20)));
SELECT arrayReduce('maxDistinctIf', arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(20)), arrayMap(x -> 1, range(20))), arrayReduce('maxDistinctOrNull', arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(20)));
SELECT k, maxDistinctArray(a), minDistinctArray(b) FROM
(
    SELECT number AS k, arrayMap(x -> [if(x = 0, nan, toFloat64(x))], range(20)) AS a, arrayMap(x -> [if(x = 0, NULL, x)], range(20)) AS b
    FROM numbers(2)
)
GROUP BY k ORDER BY k;
