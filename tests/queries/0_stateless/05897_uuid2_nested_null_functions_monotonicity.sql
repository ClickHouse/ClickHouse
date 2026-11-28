-- `ifNull` and `coalesce` of a `Nullable(Tuple(UUID1))` key with a `Tuple(UUID2)` fallback return `Tuple(UUID2)`,
-- which orders the values differently from the key. The functions must not claim monotonicity for a `UUID` nested
-- in the type of the key, so the primary key analysis gives the same result as a full scan.
-- The first granule holds the first two rows in the order of `UUID1`, and the matching value `...0002` is inside it.

DROP TABLE IF EXISTS t_uuid2_nested_null_functions_key;
CREATE TABLE t_uuid2_nested_null_functions_key (u Nullable(Tuple(UUID1))) ENGINE = MergeTree ORDER BY u
SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO t_uuid2_nested_null_functions_key VALUES
    (tuple(toUUID('00000000-0000-0001-0000-000000000001'))),
    (tuple(toUUID('00000000-0000-0000-0000-000000000002'))),
    (tuple(toUUID('00000000-0000-0001-0000-000000000003')));

SET allow_key_condition_coalesce_rewrite = 0;

SELECT 'ifNull';
SELECT count() FROM t_uuid2_nested_null_functions_key
WHERE ifNull(u, tuple(toUUID2('00000000-0000-0000-0000-000000000000'))) <= tuple(toUUID2('00000000-0000-0000-0000-000000000002'))
SETTINGS use_primary_key = 0;
SELECT count() FROM t_uuid2_nested_null_functions_key
WHERE ifNull(u, tuple(toUUID2('00000000-0000-0000-0000-000000000000'))) <= tuple(toUUID2('00000000-0000-0000-0000-000000000002'));

SELECT 'coalesce';
SELECT count() FROM t_uuid2_nested_null_functions_key
WHERE coalesce(u, tuple(toUUID2('00000000-0000-0000-0000-000000000000'))) <= tuple(toUUID2('00000000-0000-0000-0000-000000000002'))
SETTINGS use_primary_key = 0;
SELECT count() FROM t_uuid2_nested_null_functions_key
WHERE coalesce(u, tuple(toUUID2('00000000-0000-0000-0000-000000000000'))) <= tuple(toUUID2('00000000-0000-0000-0000-000000000002'));

DROP TABLE t_uuid2_nested_null_functions_key;
