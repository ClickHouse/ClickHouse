-- `hasAll` and `hasAny` of 4 and 8-byte integer arrays long enough to be searched in chunks of 512 bytes (on AArch64),
-- compared with `has`. One value is placed at every position of arrays whose lengths are around the chunk
-- boundaries, including a last chunk that overlaps the previous one, or is missing, or is at a null.

DROP TABLE IF EXISTS arrays;
CREATE TABLE arrays (a Array(Nullable(Int64)), v Int64) ENGINE = Memory;

INSERT INTO arrays
SELECT
    arrayMap(i -> if(with_nulls AND i % 7 = 3, NULL, toInt64(i * 2 + 1)), range(first_length)) AS a,
    -- `position = first_length` is a value that is not in the array.
    if(position < first_length, position * 2 + 1, 0) AS v
FROM
(
    SELECT first_length, position, with_nulls
    FROM
    (
        SELECT arrayJoin([67, 68, 69, 131, 132, 133, 135, 136, 137, 199, 263, 264, 265, 392, 393, 1000]) AS first_length
    )
    ARRAY JOIN range(first_length + 1) AS position
    ARRAY JOIN [0, 1] AS with_nulls
);

SELECT 'Int32', 'Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int32))') AS x, CAST(v, 'Int32') AS y FROM arrays);
SELECT 'Int32', 'not Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Int32)') AS x, CAST(v, 'Int32') AS y FROM arrays WHERE NOT has(a, NULL));
SELECT 'UInt32', 'Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt32))') AS x, CAST(v, 'UInt32') AS y FROM arrays);
SELECT 'UInt32', 'not Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(UInt32)') AS x, CAST(v, 'UInt32') AS y FROM arrays WHERE NOT has(a, NULL));
SELECT 'Int64', 'Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int64))') AS x, CAST(v, 'Int64') AS y FROM arrays);
SELECT 'Int64', 'not Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Int64)') AS x, CAST(v, 'Int64') AS y FROM arrays WHERE NOT has(a, NULL));
SELECT 'UInt64', 'Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt64))') AS x, CAST(v, 'UInt64') AS y FROM arrays);
SELECT 'UInt64', 'not Nullable', count(),
    countIf(hasAny(x, [y])) AS any_true, countIf(hasAny(x, [y]) != has(x, y)) AS any_mismatches,
    countIf(hasAll(x, [1, y])) AS all_true, countIf(hasAll(x, [1, y]) != (has(x, 1) AND has(x, y))) AS all_mismatches,
    countIf(hasAny(x, [y, NULL]) != (has(x, y) OR has(x, NULL))) AS any_null_mismatches,
    countIf(hasAll(x, [y, NULL]) != (has(x, y) AND has(x, NULL))) AS all_null_mismatches
FROM (SELECT CAST(a, 'Array(UInt64)') AS x, CAST(v, 'UInt64') AS y FROM arrays WHERE NOT has(a, NULL));

DROP TABLE arrays;
