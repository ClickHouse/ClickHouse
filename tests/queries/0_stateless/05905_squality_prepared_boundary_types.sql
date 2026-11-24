SET in_null_as_unknown = 1;

WITH toUUID('00000000-0000-0000-0000-000000000000') AS u
SELECT u IN materialize(toUInt128(0));
SELECT count() FROM
    (SELECT toUUID('00000000-0000-0000-0000-000000000000') AS x)
WHERE x IN materialize(toUInt128(0));
WITH toUUID('00000000-0000-0000-0000-000000000000') AS u
SELECT u IN (SELECT toUInt128(0)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_uuid_uint128_set;
CREATE TABLE squality_prepared_uuid_uint128_set (k UInt128) ENGINE = Set;
INSERT INTO squality_prepared_uuid_uint128_set SELECT toUInt128(0);
SELECT toUUID('00000000-0000-0000-0000-000000000000')
    IN squality_prepared_uuid_uint128_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_uuid_uint128_set;

WITH CAST(toFloat64(1.5) AS Variant(Float64, String)) AS v
SELECT v IN CAST('a' AS Enum8('z' = 0, 'a' = 1));
SELECT count() FROM
    (SELECT CAST(toFloat64(1.5) AS Variant(Float64, String)) AS x)
WHERE x IN CAST('a' AS Enum8('z' = 0, 'a' = 1));
WITH CAST(toFloat64(1.5) AS Variant(Float64, String)) AS v
SELECT v IN (SELECT CAST('a' AS Enum8('z' = 0, 'a' = 1))); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_variant_enum_set;
CREATE TABLE squality_prepared_variant_enum_set (k Enum8('z' = 0, 'a' = 1)) ENGINE = Set;
INSERT INTO squality_prepared_variant_enum_set VALUES ('a');
SELECT CAST(toFloat64(1.5) AS Variant(Float64, String))
    IN squality_prepared_variant_enum_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_variant_enum_set;
