-- { echoOn }

-- String keys of many lengths at fixed positions; two 17-byte keys share their first 16 bytes and alternate at one position.
SELECT arrayMap(k -> hex(k), mapKeys(m)), mapValues(m) FROM (SELECT sumMap(map('', 1, 'a', 2, 'a\0', 3, 'a\0b', 4, repeat('p', 15), 5, repeat('q', 16), 6, repeat('r', 17), 7, repeat('s', 31), 8, repeat('t', 32), 9, repeat('u', 33), 10, if(number % 2 = 0, repeat('x', 16) || 'y', repeat('x', 16) || 'z'), number, repeat('k', 100), 12, 'ключ', 13)) AS m FROM numbers(10));

-- FixedString keys, and the plain form.
SELECT arrayMap(k -> hex(k), mapKeys(m)), mapValues(m) FROM (SELECT sumMap(map(toFixedString('', 3), 1, toFixedString(toString(number % 3), 3), number, toFixedString('abc', 3), 2)) AS m FROM numbers(10));
SELECT arrayMap(k -> hex(k), mapKeys(m)), mapValues(m) FROM (SELECT sumMap(map(toFixedString('', 20), 1, toFixedString(repeat('z', 17 + number % 3), 20), number)) AS m FROM numbers(10));
SELECT sumMap(number, toString(number % 3)) FROM numbers(10);
SELECT sumMap(number, toFixedString(toString(number % 2), 2)) FROM numbers(10);

-- One state: the same keys in the same order, rotating keys, more than 16 keys per row, a key repeated within a row,
-- rows of 1 and 5 keys, and empty maps between non-empty ones.
SELECT sumMap(map('a', number, 'b', 1, 'c', 2)) FROM numbers(1000);
SELECT sumMap(mapFromArrays(arrayRotateLeft(['a', 'b', 'c', 'd'], number % 4), [1, 10, 100, 1000])) FROM numbers(1000);
SELECT sumMap(mapFromArrays(arrayMap(i -> concat('k', toString(i)), range(20)), arrayMap(i -> number + i, range(20)))) FROM numbers(100);
SELECT sumMap(map('a', number % 2 + 1, 'a', 10, 'b', 100)) FROM numbers(1000);
SELECT sumMap(mapFromArrays(arraySlice(['a', 'b', 'c', 'd', 'e'], 1, if(number % 2 = 0, 1, 5)), arraySlice([1, 2, 3, 4, 5], 1, if(number % 2 = 0, 1, 5)))) FROM numbers(1000);
SELECT sumMap(if(number % 2 = 0, map('a', number), CAST(map(), 'Map(String, UInt64)'))) FROM numbers(1000);

-- Float keys: -0 and +0, and two NaN payloads, alternate at the same position; they are one key each.
SELECT arrayMap(k -> if(isNaN(k), 'nan', if(k = 0, 'zero', toString(k))), mapKeys(m)), mapValues(m) FROM (SELECT sumMap(map(if(number % 2 = 0, 0., -0.), 1, if(number % 2 = 0, nan, reinterpretAsFloat64(toUInt64(9221120237041090561))), 10, toFloat64(number % 3), 100)) AS m FROM numbers(20));
SELECT arrayMap(k -> if(isNaN(k), 'nan', if(k = 0, 'zero', toString(k))), mapKeys(m)), mapValues(m) FROM (SELECT sumMap(map(if(number % 2 = 0, toFloat32(0.), toFloat32(-0.)), 1, if(number % 2 = 0, toFloat32(nan), reinterpretAsFloat32(toUInt32(2143289345))), 10, toFloat32(number % 3), 100)) AS m FROM numbers(20));
SELECT g, arrayMap(k -> if(isNaN(k), 'nan', if(k = 0, 'zero', toString(k))), mapKeys(m)), mapValues(m) FROM (SELECT number % 2 AS g, sumMap(map(if(intDiv(number, 2) % 2 = 0, 0., -0.), 1, if(intDiv(number, 2) % 2 = 0, nan, reinterpretAsFloat64(toUInt64(9221120237041090561))), 10)) AS m FROM numbers(20) GROUP BY g) ORDER BY g;

-- The -If combinator.
SELECT sumMapIf(map('a', number, 'b', 1), number % 3 = 0) FROM numbers(30);
SELECT number % 2 AS g, sumMapIf(map('a', number, 'b', 1), number % 3 = 0) FROM numbers(30) GROUP BY g ORDER BY g;

-- GROUP BY: groups sharing keys, interleaved and in runs, and LowCardinality keys in runs.
SELECT number % 3 AS g, sumMap(map('a', number, 'b', 1, toString(number % 5), 2)) FROM numbers(60) GROUP BY g ORDER BY g;
SELECT intDiv(number, 4) % 3 AS g, sumMap(map('a', number, 'b', 1, toString(number % 5), 2)) FROM numbers(60) GROUP BY g ORDER BY g;
SELECT intDiv(number, 4) % 3 AS g, sumMap(CAST(map('a', number, 'b', 1, toString(number % 5), 2), 'Map(LowCardinality(String), UInt64)')) FROM numbers(60) GROUP BY g ORDER BY g;

-- Other nested functions, and a nested state.
SELECT uniqMap(map('a', number % 7, 'b', 1)), maxMap(map('a', number, 'b', 1)), avgMap(map('a', number, 'b', 1)) FROM numbers(100);
SELECT mapKeys(m), arrayMap(v -> arraySort(v), mapValues(m)) FROM (SELECT groupArrayMap(map('a', number, 'b', number % 3)) AS m FROM numbers(10));
SELECT quantilesExactMap(0.5)(map('a', number, 'b', 1)) FROM numbers(101);
SELECT mapKeys(x), arrayMap(v -> finalizeAggregation(v), mapValues(x)) FROM (SELECT sumStateMap(map('a', number, 'b', 1)) AS x FROM numbers(100));

-- -State and -Merge, external aggregation and remote.
SELECT sumMapMerge(s) FROM (SELECT number % 5 AS g, sumMapState(map('a', number, repeat('w', 30), 1, toString(number % 7), 2)) AS s FROM numbers(100) GROUP BY g);
SELECT number % 5 AS g, sumMap(map('a', number, repeat('w', 30), 1, toString(number % 7), 2)) FROM numbers(100) GROUP BY g ORDER BY g SETTINGS max_bytes_before_external_group_by = 1;
SELECT sumMap(map(toString(number % 3), number, repeat('w', 30), 1)) FROM remote('127.0.0.{1,2}', numbers(10));

-- A window frame.
SELECT n, sumMap(m) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (SELECT number AS n, map(toString(number % 3), number, 'all', 1) AS m FROM numbers(6)) ORDER BY n;

-- -ForEach over arrays of maps of growing length.
SELECT sumMapForEach(arrayMap(i -> map(toString(i), number, 'x', 1), range(number % 5))) FROM numbers(20);

-- AggregatingMergeTree merges.
DROP TABLE IF EXISTS t_map_repeated_keys;
CREATE TABLE t_map_repeated_keys (g UInt64, s SimpleAggregateFunction(sumMap, Map(String, UInt64)), a AggregateFunction(sumMap, Map(String, UInt64))) ENGINE = AggregatingMergeTree ORDER BY g;
INSERT INTO t_map_repeated_keys SELECT number % 2, map('a', number, repeat('v', 40), 1), sumMapState(map('a', number, repeat('v', 40), 1)) FROM numbers(10) GROUP BY number;
INSERT INTO t_map_repeated_keys SELECT number % 2, map('a', number, 'b', 2), sumMapState(map('a', number, 'b', 2)) FROM numbers(10) GROUP BY number;
INSERT INTO t_map_repeated_keys SELECT number % 2, map(repeat('v', 40), number, 'b', 3), sumMapState(map(repeat('v', 40), number, 'b', 3)) FROM numbers(10) GROUP BY number;
OPTIMIZE TABLE t_map_repeated_keys FINAL;
SELECT g, s, finalizeAggregation(a) FROM t_map_repeated_keys ORDER BY g;
DROP TABLE t_map_repeated_keys;

-- Empty states.
SELECT g, sumMap(if(g = 1, map('a', number), CAST(map(), 'Map(String, UInt64)'))) FROM (SELECT number % 3 AS g, number FROM numbers(30)) GROUP BY g ORDER BY g;
SELECT sumMapMerge(s) FROM (SELECT sumMapState(CAST(map(), 'Map(String, UInt64)')) AS s UNION ALL SELECT sumMapState(map('a', toUInt64(1))));
