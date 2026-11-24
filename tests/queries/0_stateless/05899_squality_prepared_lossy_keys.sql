SET in_null_as_unknown = 1;

SELECT toFloat64(1.5) IN CAST('a' AS Enum8('a' = 1));
SELECT count() FROM values('x Float64', (1.5)) WHERE x IN CAST('a' AS Enum8('a' = 1));
SELECT toFloat64(1.5) IN (SELECT CAST('a' AS Enum8('a' = 1))); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_enum_set;
CREATE TABLE squality_prepared_enum_set (k Enum8('a' = 1)) ENGINE = Set;
INSERT INTO squality_prepared_enum_set VALUES ('a');
SELECT toFloat64(1.5) IN squality_prepared_enum_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_enum_set;

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
