-- Conversions between `UUID` and `UUID2` keep the identifier, but change its representation and its ordering.

-- A default value of `groupArrayInsertAt` of the other UUID type.
SELECT groupArrayInsertAt(toUUID('00000000-0000-0000-0000-000000000001'), 2)(toUUID2('00000000-0000-0000-0000-000000000001'), toUInt64(1));
SELECT groupArrayInsertAt(toUUID2('00000000-0000-0000-0000-000000000001'), 2)(toUUID('00000000-0000-0000-0000-000000000001'), toUInt64(1));
SELECT groupArrayInsertAt([toUUID2('00000000-0000-0000-0000-000000000001')], 2)([toUUID2('00000000-0000-0000-0000-000000000002')], toUInt64(1));

-- `transform` and simple `CASE` keep the mapping of a value of the other UUID type.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    transform(toUUID2(s), [toUUID(s)], [7], 0),
    CASE toUUID2(s) WHEN toUUID(s) THEN 7 ELSE 0 END,
    transform(toUUID(s), [toUUID2(s)], [7], 0),
    CASE toUUID(s) WHEN toUUID2(s) THEN 7 ELSE 0 END;

-- A range comparison with the other UUID type is evaluated in the order of `UUID2`.
SELECT count()
FROM values('u UUID1', toUUID('00000000-0000-0000-0000-000000000002'))
WHERE u = toUUID('00000000-0000-0000-0000-000000000002') AND u < toUUID2('00000000-0000-0001-0000-000000000001')
SETTINGS optimize_redundant_comparisons = 1;
SELECT count()
FROM values('u UUID1', toUUID('00000000-0000-0000-0000-000000000002'))
WHERE u = toUUID('00000000-0000-0000-0000-000000000002') AND u < toUUID2('00000000-0000-0001-0000-000000000001')
SETTINGS optimize_redundant_comparisons = 0;

-- `ifNull` and `coalesce` of a `Nullable(UUID1)` key with a `UUID2` fallback return `UUID2`, which orders the values
-- differently, so the primary key analysis must give the same result as a full scan.
DROP TABLE IF EXISTS t_uuid2_null_functions_key;
CREATE TABLE t_uuid2_null_functions_key (u Nullable(UUID1)) ENGINE = MergeTree ORDER BY u
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_uuid2_null_functions_key VALUES
    ('00000000-0000-0001-0000-000000000001'), ('00000000-0000-0000-0000-000000000002'), ('00000000-0000-0002-0000-000000000000');
SELECT
    countIf(ifNull(u, toUUID2('00000000-0000-0000-0000-000000000000')) <= toUUID2('00000000-0000-0000-0000-000000000002')),
    countIf(coalesce(u, toUUID2('00000000-0000-0000-0000-000000000000')) <= toUUID2('00000000-0000-0000-0000-000000000002'))
FROM t_uuid2_null_functions_key SETTINGS use_primary_key = 0;
SELECT count() FROM t_uuid2_null_functions_key
WHERE ifNull(u, toUUID2('00000000-0000-0000-0000-000000000000')) <= toUUID2('00000000-0000-0000-0000-000000000002');
SELECT count() FROM t_uuid2_null_functions_key
WHERE coalesce(u, toUUID2('00000000-0000-0000-0000-000000000000')) >= toUUID2('00000000-0000-0001-0000-000000000000');
SELECT count() FROM t_uuid2_null_functions_key
WHERE coalesce(u, toUUID2('00000000-0000-0000-0000-000000000000')) >= toUUID2('00000000-0000-0001-0000-000000000000')
SETTINGS use_primary_key = 0;
DROP TABLE t_uuid2_null_functions_key;
