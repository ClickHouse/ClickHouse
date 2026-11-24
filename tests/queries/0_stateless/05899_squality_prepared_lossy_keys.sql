SET in_null_as_unknown = 1;

SELECT toFloat64(1.5) IN CAST('a' AS Enum8('a' = 1));
SELECT count() FROM values('x Float64', (1.5)) WHERE x IN CAST('a' AS Enum8('a' = 1));
SELECT toFloat64(1.5) IN (SELECT CAST('a' AS Enum8('a' = 1))); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_enum_set;
CREATE TABLE squality_prepared_enum_set (k Enum8('a' = 1)) ENGINE = Set;
INSERT INTO squality_prepared_enum_set VALUES ('a');
SELECT toFloat64(1.5) IN squality_prepared_enum_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_enum_set;

SELECT [toFloat64(1.5)] IN [CAST('a' AS Enum8('a' = 1))];
SELECT count() FROM (SELECT [toFloat64(1.5)] AS x)
WHERE x IN [CAST('a' AS Enum8('a' = 1))];
SELECT [toFloat64(1.5)] IN
    (SELECT [CAST('a' AS Enum8('a' = 1))]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_enum_array_set;
CREATE TABLE squality_prepared_enum_array_set (k Array(Enum8('a' = 1))) ENGINE = Set;
INSERT INTO squality_prepared_enum_array_set SELECT [CAST('a' AS Enum8('a' = 1))];
SELECT [toFloat64(1.5)] IN squality_prepared_enum_array_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_enum_array_set;

SELECT toUInt32(1) IN materialize(toIPv4(1));
SELECT count() FROM (SELECT toUInt32(1) AS x) WHERE x IN materialize(toIPv4(1));
SELECT toUInt32(1) IN (SELECT toIPv4(1)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_ipv4_set;
CREATE TABLE squality_prepared_ipv4_set (k IPv4) ENGINE = Set;
INSERT INTO squality_prepared_ipv4_set SELECT toIPv4(1);
SELECT toUInt32(1) IN squality_prepared_ipv4_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_ipv4_set;

SELECT CAST('big' AS Enum16('small' = 1, 'big' = 257))
    IN CAST('other' AS Enum8('other' = 1));
SELECT count() FROM
    (SELECT CAST('big' AS Enum16('small' = 1, 'big' = 257)) AS x)
WHERE x IN CAST('other' AS Enum8('other' = 1));
SELECT CAST('big' AS Enum16('small' = 1, 'big' = 257))
    IN (SELECT CAST('other' AS Enum8('other' = 1))); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_enum_narrow_set;
CREATE TABLE squality_prepared_enum_narrow_set (k Enum8('other' = 1)) ENGINE = Set;
INSERT INTO squality_prepared_enum_narrow_set VALUES ('other');
SELECT CAST('big' AS Enum16('small' = 1, 'big' = 257))
    IN squality_prepared_enum_narrow_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_enum_narrow_set;

SELECT [CAST('big' AS Enum16('small' = 1, 'big' = 257))]
    IN [CAST('other' AS Enum8('other' = 1))];
SELECT [CAST('big' AS Enum16('small' = 1, 'big' = 257))]
    IN (SELECT [CAST('other' AS Enum8('other' = 1))]); -- { serverError NOT_IMPLEMENTED }

SELECT CAST(toUInt64(1) AS Variant(UInt64, String)) IN '1';
SELECT count() FROM
    (SELECT CAST(toUInt64(1) AS Variant(UInt64, String)) AS x)
WHERE x IN '1';
SELECT CAST(toUInt64(1) AS Variant(UInt64, String))
    IN (SELECT '1'); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_string_set;
CREATE TABLE squality_prepared_string_set (k String) ENGINE = Set;
INSERT INTO squality_prepared_string_set VALUES ('1');
SELECT CAST(toUInt64(1) AS Variant(UInt64, String))
    IN squality_prepared_string_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_string_set;

SELECT [CAST(toUInt64(1) AS Variant(UInt64, String))] IN materialize(['1']);
SELECT [CAST(toUInt64(1) AS Variant(UInt64, String))]
    IN (SELECT ['1']); -- { serverError NOT_IMPLEMENTED }

SELECT [toDecimal64('0.01', 2)] IN [toDateTime64('1970-01-01 00:00:00', 1, 'UTC')],
       [toDecimal64('0.01', 2)] IN materialize([toDateTime64('1970-01-01 00:00:00', 1, 'UTC')]);
SELECT count() FROM (SELECT [toDecimal64('0.01', 2)] AS a)
WHERE a IN materialize([toDateTime64('1970-01-01 00:00:00', 1, 'UTC')]);
SELECT [toDecimal64('0.01', 2)] IN
    (SELECT [toDateTime64('1970-01-01 00:00:00', 1, 'UTC')]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_temporal_set;
CREATE TABLE squality_prepared_temporal_set (k Array(DateTime64(1, 'UTC'))) ENGINE = Set;
INSERT INTO squality_prepared_temporal_set
SELECT [toDateTime64('1970-01-01 00:00:00', 1, 'UTC')];
SELECT [toDecimal64('0.01', 2)] IN squality_prepared_temporal_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_temporal_set;
