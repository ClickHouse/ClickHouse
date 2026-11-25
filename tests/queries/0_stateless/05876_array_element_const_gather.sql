-- `arrayElement` of a constant array with a non-constant index gathers the rows with `index`, from the elements of the
-- constant array, or from the elements followed by a default value when some indices are out of range.

SELECT 'small array, mixed';
SELECT [[1], [2, 3], []][number % 5 - 1], arrayElementOrNull([[1], [2, 3], []], number % 5 - 1) FROM numbers(6);
SELECT ['a', 'bb'][-number], [map('k', 1), map()][number + 1], [(1, 'x'), (2, 'y')][number] FROM numbers(4);

SELECT 'small array, all in range and all out of range';
SELECT [10, 20, 30][number % 3 + 1], [[1], [2]][number % 2 + 1] FROM numbers(4);
SELECT [[1], [2]][number + 3], ['a', 'b'][number + 3], arrayElementOrNull([[1], [2]], number + 3) FROM numbers(2);

-- A constant array larger than the positions of a block: only the selected elements are copied.
SELECT 'large array';
SELECT arrayMap(x -> toString(x), range(100000))[number % 7 - 2] FROM numbers(7) SETTINGS max_block_size = 3;
SELECT sum(cityHash64(arrayMap(x -> toString(x), range(100000))[number % 7 - 2])) FROM numbers(100000) SETTINGS max_block_size = 1000;
SELECT sum(cityHash64(arrayMap(x -> toString(x), range(100000))[number % 7 + 1])) FROM numbers(100000) SETTINGS max_block_size = 1000;
SELECT arrayElementOrNull(arrayMap(x -> [x], range(50000)), toInt64(number) - 2) FROM numbers(5) SETTINGS max_block_size = 2;

-- `replicate` and `index` of arrays of arrays: the elements of short arrays are gathered with `index`, long arrays are
-- copied row by row without an index of every element.
SELECT 'replicate and index';
SELECT materialize([[1, 2], [3]]) FROM numbers(3);
SELECT x, a FROM (SELECT [range(number), [number]] AS a, range(number % 3) AS r FROM numbers(5)) ARRAY JOIN r AS x ORDER BY x, a;
SELECT sum(cityHash64(toString(a))) FROM (SELECT [range(number % 100), [number]] AS a, range(number % 3) AS r FROM numbers(5000)) ARRAY JOIN r AS x;
SELECT sum(cityHash64(toString(a))) FROM (SELECT [range(number % 40)] AS a FROM numbers(20000) ORDER BY cityHash64(number));
SELECT count() FROM (SELECT materialize([range(100000)::Array(UInt8)]) AS a FROM numbers(128)) WHERE NOT ignore(a)
SETTINGS max_memory_usage = 50000000, max_block_size = 128;
