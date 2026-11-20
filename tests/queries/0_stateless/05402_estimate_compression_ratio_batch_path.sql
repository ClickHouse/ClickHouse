-- The batch path of `estimateCompressionRatio` serializes a whole range at once. With `-If` it must skip
-- the same rows as `add`, so compare it with aggregation that calls `add` row by row: `GROUP BY` with the
-- optimization for consecutive equal keys turned off.
-- One thread and no external aggregation, so all values go to one state.

SET max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET min_hit_rate_to_use_consecutive_keys_optimization = 2;

SELECT
    (SELECT estimateCompressionRatioIf(number, number < 500) FROM numbers(1000))
    = (SELECT estimateCompressionRatioIf(number, number < 500) FROM numbers(1000) GROUP BY number % 1);

-- `arrayReduce` adds an array element by element, as `canOptimizeEqualKeysRanges` is false, also with `-If`.
SELECT arrayReduce('estimateCompressionRatioIf', arrayMap(x -> toUInt64(x), range(100)), arrayMap(x -> toUInt8(0), range(100)));

SELECT
    arrayReduce('estimateCompressionRatioIf', arrayMap(x -> toUInt64(x), range(100)), arrayMap(x -> toUInt8(x < 50), range(100)))
    = (SELECT estimateCompressionRatioIf(number, number < 50) FROM numbers(100) GROUP BY number % 1);

-- The result for one array must not depend on the other rows of its block: the serialization prefix of
-- `Dynamic` and `JSON` lists the types and paths it contains.
SELECT
    (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatio', arrayMap(x -> if(number % 2 = 0, CAST(x AS Dynamic), CAST(toString(x) AS Dynamic)), range(16))))) FROM numbers(10) SETTINGS max_block_size = 1)
    = (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatio', arrayMap(x -> if(number % 2 = 0, CAST(x AS Dynamic), CAST(toString(x) AS Dynamic)), range(16))))) FROM numbers(10) SETTINGS max_block_size = 10);

SELECT
    (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatio', arrayMap(x -> CAST(concat('{"k', toString(number), '": ', toString(x), '}') AS JSON), range(16))))) FROM numbers(10) SETTINGS max_block_size = 1)
    = (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatio', arrayMap(x -> CAST(concat('{"k', toString(number), '": ', toString(x), '}') AS JSON), range(16))))) FROM numbers(10) SETTINGS max_block_size = 10);

SELECT
    (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatioOrNull', arrayMap(x -> if(number % 2 = 0, CAST(x AS Dynamic), CAST(toString(x) AS Dynamic)), range(16))))) FROM numbers(10) SETTINGS max_block_size = 1)
    = (SELECT sum(cityHash64(number, arrayReduce('estimateCompressionRatioOrNull', arrayMap(x -> if(number % 2 = 0, CAST(x AS Dynamic), CAST(toString(x) AS Dynamic)), range(16))))) FROM numbers(10) SETTINGS max_block_size = 10);
