-- Sorts of strings compare 8-byte prefix keys first and whole strings only on ties.
-- Check them against the order of byte arrays, which does not use any string sorting code.
-- The strings are built from a small alphabet with zero bytes and bytes >= 0x80, with lengths around 8 bytes
-- and long common prefixes, so that many keys tie and many strings differ only after the prefix or in length.

DROP VIEW IF EXISTS strings;
CREATE VIEW strings AS
SELECT
    toUInt32(number) AS i,
    concat(
        ['', 'abcdefgh', 'abcdefg', 'abcdefghi'][1 + cityHash64(number, 'p') % 4],
        arrayStringConcat(arrayMap(j -> ['\0', 'a', 'b', '\xFF', ' '][1 + cityHash64(number, j) % 5], range(cityHash64(number) % 13)))) AS s,
    if(number % 11 = 0, NULL, s) AS ns,
    toLowCardinality(s) AS lcs,
    toFixedString(substring(s, 1, 10), 10) AS fs10,
    toFixedString(substring(s, 1, 3), 3) AS fs3,
    arrayMap(k -> reinterpretAsUInt8(substring(s, k, 1)), range(1, length(s) + 1)) AS bytes
FROM numbers(20000);

SELECT 'ORDER BY';
SELECT (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY s)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes));
SELECT (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY s DESC)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes DESC));
SELECT (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY s LIMIT 100)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes LIMIT 100));
SELECT (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY s DESC LIMIT 100)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes DESC LIMIT 100));
SELECT (SELECT groupArray((s, i)) FROM (SELECT s, i FROM strings ORDER BY s, i)) = (SELECT groupArray((s, i)) FROM (SELECT s, i FROM strings ORDER BY bytes, i));
SELECT (SELECT groupArray((s, i)) FROM (SELECT s, i FROM strings ORDER BY s DESC, i)) = (SELECT groupArray((s, i)) FROM (SELECT s, i FROM strings ORDER BY bytes DESC, i));
SELECT (SELECT groupArray((i % 7, s)) FROM (SELECT s, i FROM strings ORDER BY i % 7, s)) = (SELECT groupArray((i % 7, s)) FROM (SELECT s, i FROM strings ORDER BY i % 7, bytes));
SELECT (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY s SETTINGS max_block_size = 997)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes));
SELECT (SELECT groupArray(ns) FROM (SELECT ns FROM strings ORDER BY ns NULLS LAST)) = (SELECT groupArray(ns) FROM (SELECT ns FROM strings ORDER BY ns IS NULL, bytes));
SELECT (SELECT groupArray(ns) FROM (SELECT ns FROM strings ORDER BY ns DESC NULLS FIRST)) = (SELECT groupArray(ns) FROM (SELECT ns FROM strings ORDER BY ns IS NOT NULL, bytes DESC));
SELECT (SELECT groupArray(lcs) FROM (SELECT lcs FROM strings ORDER BY lcs)) = (SELECT groupArray(s) FROM (SELECT s FROM strings ORDER BY bytes));
SELECT (SELECT groupArray(fs10) FROM (SELECT fs10 FROM strings ORDER BY fs10)) = (SELECT groupArray(fs10) FROM (SELECT fs10 FROM strings ORDER BY arrayMap(k -> reinterpretAsUInt8(substring(fs10, k, 1)), range(1, 11))));
SELECT (SELECT groupArray(fs3) FROM (SELECT fs3 FROM strings ORDER BY fs3 DESC)) = (SELECT groupArray(fs3) FROM (SELECT fs3 FROM strings ORDER BY arrayMap(k -> reinterpretAsUInt8(substring(fs3, k, 1)), range(1, 4)) DESC));

-- Arrays of different sizes: large ones use the prefix keys, small ones do not.
SELECT 'arraySort';
SELECT DISTINCT arraySort(arr) = ref, arrayReverseSort(arr) = arrayReverse(ref)
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arraySort(arr) = ref, arrayReverseSort(arr) = arrayReverse(ref)
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 3000);
SELECT DISTINCT arraySlice(arrayPartialSort(10, arr), 1, 10) = arraySlice(ref, 1, 10), arraySlice(arrayPartialReverseSort(10, arr), 1, 10) = arraySlice(arrayReverse(ref), 1, 10)
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arraySort(x -> x, arr) = ref, arraySort(x -> (x, 1), arr) = ref
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arraySort(arr) = ref, arrayReverseSort(arr) = arrayReverse(ref)
FROM (SELECT groupArray(fs10) AS arr, arraySort(x -> arrayMap(k -> reinterpretAsUInt8(substring(x, k, 1)), range(1, 11)), arr) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arrayMap(x -> toString(x), arraySort(arr)) = ref, arrayMap(x -> toString(x), arrayReverseSort(arr)) = arrayReverse(ref)
FROM (SELECT groupArray(lcs) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT
    arrayFilter(x -> x IS NOT NULL, arraySort(arr)) = ref,
    arrayFilter(x -> x IS NOT NULL, arrayReverseSort(arr)) = arrayReverse(ref),
    arrayAll(x -> x IS NULL, arraySlice(arraySort(arr), length(ref) + 1)),
    arrayAll(x -> x IS NULL, arraySlice(arrayReverseSort(arr), length(ref) + 1))
FROM
(
    SELECT
        arrayMap(x -> x.1, groupArray(tuple(ns))) AS arr,
        arrayMap(x -> x.2, arraySort(x -> x.1, groupArrayIf((bytes, s), ns IS NOT NULL))) AS ref
    FROM strings GROUP BY i % 10
);

SELECT 'arrayTopK';
SELECT DISTINCT arrayBottomK(50, arr) = arraySlice(ref, 1, 50), arrayTopK(50, arr) = arraySlice(arrayReverse(ref), 1, 50)
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arrayBottomK(5000, arr) = ref
FROM (SELECT groupArray(s) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);
SELECT DISTINCT arrayMap(x -> toString(x), arrayBottomK(50, arr)) = arraySlice(ref, 1, 50)
FROM (SELECT groupArray(lcs) AS arr, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref FROM strings GROUP BY i % 10);

SELECT 'mapSort';
SELECT DISTINCT mapKeys(mapSort(m)) = ref, mapKeys(mapReverseSort(m)) = arrayReverse(ref)
FROM
(
    SELECT
        mapFromArrays(groupArray(s), groupArray(i)) AS m,
        arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref
    FROM (SELECT any(i) AS i, s, any(bytes) AS bytes FROM strings GROUP BY s)
    GROUP BY i % 10
);
SELECT DISTINCT arrayMap(x -> toString(x), mapKeys(mapSort(m))) = ref
FROM
(
    SELECT
        mapFromArrays(groupArray(toLowCardinality(s)), groupArray(i)) AS m,
        arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))) AS ref
    FROM (SELECT any(i) AS i, s, any(bytes) AS bytes FROM strings GROUP BY s)
    GROUP BY i % 10
);

SELECT 'groupArraySorted';
SELECT DISTINCT groupArraySorted(100)(s) = arraySlice(arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((bytes, s)))), 1, 100) FROM strings GROUP BY i % 10;

DROP VIEW strings;
