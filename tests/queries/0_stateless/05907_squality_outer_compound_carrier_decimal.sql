SET in_null_as_unknown = 1;

SELECT map('a', toFloat64(1)) IN
    (materialize(map('a', CAST(toDecimal32(1, 0) AS Dynamic))), NULL),
    map('a', toFloat64(1)) IN
    (materialize(CAST(map('a', CAST(toDecimal32(1, 0) AS Dynamic)) AS Dynamic)), NULL);
SELECT count() FROM (SELECT map('a', toFloat64(1)) AS x)
WHERE x IN
    (materialize(CAST(map('a', CAST(toDecimal32(1, 0) AS Dynamic)) AS Dynamic)), NULL);

DROP TABLE IF EXISTS squality_outer_map_carrier_constraint;
CREATE TABLE squality_outer_map_carrier_constraint
(
    a Map(String, Float64),
    b Dynamic,
    CONSTRAINT matching_decimal CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_outer_map_carrier_constraint
SELECT map('a', toFloat64(1)),
    CAST(map('a', CAST(toDecimal32(1, 0) AS Dynamic)) AS Dynamic);
SELECT count() FROM squality_outer_map_carrier_constraint;
DROP TABLE squality_outer_map_carrier_constraint;

SELECT [toFloat64(1)] IN
    (materialize([CAST(toDecimal32(1, 0) AS Dynamic)]), NULL),
    [toFloat64(1)] IN
    (materialize(CAST([CAST(toDecimal32(1, 0) AS Dynamic)] AS Dynamic)), NULL);
SELECT count() FROM (SELECT [toFloat64(1)] AS x)
WHERE x IN
    (materialize(CAST([CAST(toDecimal32(1, 0) AS Dynamic)] AS Dynamic)), NULL);

DROP TABLE IF EXISTS squality_outer_array_carrier_constraint;
CREATE TABLE squality_outer_array_carrier_constraint
(
    a Array(Float64),
    b Dynamic,
    CONSTRAINT matching_decimal CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_outer_array_carrier_constraint
SELECT [toFloat64(1)],
    CAST([CAST(toDecimal32(1, 0) AS Dynamic)] AS Dynamic);
SELECT count() FROM squality_outer_array_carrier_constraint;
DROP TABLE squality_outer_array_carrier_constraint;
