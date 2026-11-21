-- Filter of `Array(LowCardinality(String))` after PREWHERE, checked against an `Array(String)` copy of the same data.
DROP TABLE IF EXISTS t_array_lc_filter;

CREATE TABLE t_array_lc_filter (k UInt64, arr Array(LowCardinality(String)), arr_str Array(String))
ENGINE = MergeTree ORDER BY k;

-- Every fifth array is empty.
INSERT INTO t_array_lc_filter
SELECT number, a, a FROM (SELECT number, arrayMap(x -> toString(number + x), range(number % 5)) AS a FROM numbers(10000));

SELECT count(), groupArray(arr) = groupArray(arr_str) FROM t_array_lc_filter PREWHERE k % 3 = 0;

DROP TABLE t_array_lc_filter;

-- Filter of an in-memory `Array(LowCardinality(String))` column.
SELECT arr FROM (SELECT CAST(arrayMap(x -> toString(x), range(number % 4)), 'Array(LowCardinality(String))') AS arr FROM numbers(8)) WHERE length(arr) != 2;
