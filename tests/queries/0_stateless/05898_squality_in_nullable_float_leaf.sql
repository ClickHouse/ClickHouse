SET in_null_as_unknown = 1;

SELECT
    [toFloat64('nan')] IN [toNullable(toFloat64('nan'))],
    [toFloat64('nan')] IN materialize([toNullable(toFloat64('nan'))]);

SELECT count() FROM
    (SELECT [toFloat64('nan')] AS a, [toNullable(toFloat64('nan'))] AS b)
WHERE a IN b;

SELECT count() FROM
    (SELECT [toFloat64('nan')] AS a, [toNullable(toFloat64('nan'))] AS b)
WHERE a IN materialize(b);

DROP TABLE IF EXISTS squality_nullable_float_constraint;
CREATE TABLE squality_nullable_float_constraint
(
    a Array(Float64),
    b Array(Nullable(Float64)),
    CONSTRAINT c CHECK a IN b
) ENGINE = Memory;
INSERT INTO squality_nullable_float_constraint
SELECT [toFloat64('nan')], [toNullable(toFloat64('nan'))]; -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_nullable_float_constraint;
