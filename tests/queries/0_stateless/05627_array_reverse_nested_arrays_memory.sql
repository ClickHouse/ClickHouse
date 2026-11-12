-- The generic paths of `arrayReverse` and `array` copy nested arrays by ranges: an index entry for every element
-- of a long nested array would take several times the memory of the data.

SELECT length(arrayReverse(materialize([arrayWithConstant(10000000, toUInt8(1))])))
FROM numbers(1) SETTINGS max_threads = 1, max_memory_usage = '64Mi';

SELECT length([materialize(arrayWithConstant(5000000, toUInt8(1))), materialize(arrayWithConstant(5000000, toUInt8(1)))])
FROM numbers(1) SETTINGS max_threads = 1, max_memory_usage = '64Mi';

-- Results of the paths that copy by ranges, also through tuples, maps and `Nullable`.
SELECT arrayReverse([[1, 2], [3], [], [4, 5, 6]]);
SELECT arrayReverse(materialize([[1, 2], [3]])) FROM numbers(2);
SELECT arrayReverse([[[1], [2, 3]], [[4]], []]);
SELECT arrayReverse([([1, 2], 'a'), ([3], 'b')]);
SELECT arrayReverse([map(1, [1, 2]), map(2, [3])]);
SELECT arrayReverse([(1, [toNullable(1), NULL]), (2, [])]);
SELECT arrayReverse([[1]::Array(Nullable(UInt8)), [NULL]]);
SELECT arrayReverse(CAST([1, 2, 3], 'Array(Dynamic)'));
SELECT arrayReverse(materialize(CAST([], 'Array(Array(UInt8))')));

SELECT [[number], [number + 1, number]] FROM numbers(3);
SELECT [materialize([1, 2]), materialize([3]), materialize(CAST([], 'Array(UInt8)'))];
SELECT [[number]::Array(Nullable(UInt64)), [NULL]] FROM numbers(2);
SELECT [[[number]], [[number + 1], []]] FROM numbers(2);
