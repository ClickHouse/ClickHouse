-- timeSeriesStoreTags(id, ...) keeps the mapping id -> tags in the query context and timeSeriesIdToTags(id)
-- reads it back. Expected behaviour: the FunctionDocumentation blocks of
-- src/Functions/TimeSeries/timeSeriesStoreTags.cpp and src/Functions/TimeSeries/timeSeriesIdToTags.cpp --
-- "Identifier of a time series. Can be of any comparable type". The collector keeps several internal maps,
-- one per identifier layout, and the storing side and the reading side must pick the same one, so every row
-- below has to come back with exactly the tags it was stored with. The identifiers are derived from `number`
-- so that they are real columns and not constants.

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, toUInt128(number) + 1000 AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint128', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, (toUInt32(number + 1), toUInt64(number) + 2) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint32_uint64', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, (toUInt64(number) + 3, toUInt128(number) + 4) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_uint128', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, (toUInt64(number) + 5, reinterpretAsFixedString(toUInt128(number) + 6)) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_fixedstring16', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, (toUInt64(number) + 7, reinterpretAsFixedString(toUInt64(number) + 8)) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_fixedstring8', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(id), timeSeriesIdToTags(id)
FROM (SELECT number AS n, (toUInt64(number) + 9, toString(number)) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_string', 'row', toString(number)) AS id
      FROM numbers(2))
ORDER BY n;

-- A dictionary-encoded second component must share its map with the plain layout: the identifiers below are
-- stored through a plain tuple and read back through a tuple whose second component is LowCardinality.

SELECT toTypeName(key), toTypeName(lc_id), timeSeriesIdToTags(lc_id)
FROM (SELECT number AS n, (toUInt64(number) + 10, toUInt64(number) + 11) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_lc_uint64', 'row', toString(number)) AS id,
             (id.1, toLowCardinality(id.2)) AS lc_id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(lc_id), timeSeriesIdToTags(lc_id)
FROM (SELECT number AS n, (toUInt64(number) + 12, toUInt128(number) + 13) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_lc_uint128', 'row', toString(number)) AS id,
             (id.1, toLowCardinality(id.2)) AS lc_id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(lc_id), timeSeriesIdToTags(lc_id)
FROM (SELECT number AS n, (toUInt64(number) + 14, reinterpretAsFixedString(toUInt128(number) + 15)) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_lc_fixedstring16', 'row', toString(number)) AS id,
             (id.1, toLowCardinality(id.2)) AS lc_id
      FROM numbers(2))
ORDER BY n;

SELECT toTypeName(key), toTypeName(lc_id), timeSeriesIdToTags(lc_id)
FROM (SELECT number AS n, (toUInt64(number) + 16, toNullable(toUInt64(number) + 17)) AS key,
             timeSeriesStoreTags(key, [], '__name__', 'uint64_lc_nullable_uint64', 'row', toString(number)) AS id,
             (id.1, toLowCardinality(id.2)) AS lc_id
      FROM numbers(2))
ORDER BY n;
