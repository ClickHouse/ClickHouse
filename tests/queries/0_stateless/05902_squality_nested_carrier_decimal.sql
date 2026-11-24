SET in_null_as_unknown = 1;

SELECT map('a', toFloat64(1)) IN (materialize(map('a', toDecimal32(1, 0))), NULL),
       map('a', toFloat64(1)) IN
           (materialize(map('a', CAST(toDecimal32(1, 0) AS Dynamic))), NULL);

SELECT count() FROM (SELECT map('a', toFloat64(1)) AS x)
WHERE x IN (materialize(map('a', CAST(toDecimal32(1, 0) AS Dynamic))), NULL);

DROP TABLE IF EXISTS squality_nested_carrier_constraint;
CREATE TABLE squality_nested_carrier_constraint
(
    a Map(String, Float64),
    b Map(String, Dynamic),
    CONSTRAINT matching_decimal CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_nested_carrier_constraint
SELECT map('a', toFloat64(1)), map('a', CAST(toDecimal32(1, 0) AS Dynamic));
SELECT count() FROM squality_nested_carrier_constraint;
DROP TABLE squality_nested_carrier_constraint;

SELECT [toFloat64(1)] IN (materialize([toDecimal32(1, 0)]), NULL),
       [toFloat64(1)] IN (materialize([CAST(toDecimal32(1, 0) AS Dynamic)]), NULL);
SELECT count() FROM (SELECT [toFloat64(1)] AS x)
WHERE x IN (materialize([CAST(toDecimal32(1, 0) AS Dynamic)]), NULL);

DROP TABLE IF EXISTS squality_nested_array_constraint;
CREATE TABLE squality_nested_array_constraint
(
    a Array(Float64),
    b Array(Dynamic),
    CONSTRAINT matching_decimal CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_nested_array_constraint
SELECT [toFloat64(1)], [CAST(toDecimal32(1, 0) AS Dynamic)];
SELECT count() FROM squality_nested_array_constraint;
DROP TABLE squality_nested_array_constraint;

SELECT map('a', toDate('1970-01-02')) IN
    (materialize(map('a', CAST(toDateTime(1, 'UTC') AS Dynamic))), NULL); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM (SELECT map('a', toDate('1970-01-02')) AS x)
WHERE x IN (materialize(map('a', CAST(toDateTime(1, 'UTC') AS Dynamic))), NULL); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_nested_calendar_constraint;
CREATE TABLE squality_nested_calendar_constraint
(
    a Map(String, Date),
    b Map(String, Dynamic),
    CONSTRAINT deferred_calendar CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_nested_calendar_constraint
SELECT map('a', toDate('1970-01-02')),
       map('a', CAST(toDateTime(1, 'UTC') AS Dynamic)); -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_nested_calendar_constraint;
