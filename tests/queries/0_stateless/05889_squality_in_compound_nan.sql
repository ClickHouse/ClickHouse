SELECT
    [toFloat64(toFloat32('nan'))] IN ([toFloat32('nan')], NULL),
    [toFloat64(toFloat32('nan'))] IN (materialize([toFloat32('nan')]), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM (SELECT [toFloat64(toFloat32('nan'))] AS a)
WHERE a IN ([toFloat32('nan')], NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM (SELECT [toFloat64(toFloat32('nan'))] AS a)
WHERE a IN (materialize([toFloat32('nan')]), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    [toFloat64('nan')] IN ([toFloat64('nan')], NULL),
    [toFloat64('nan')] IN (materialize([toFloat64('nan')]), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM (SELECT [toFloat64('nan')] AS a)
WHERE a IN ([toFloat64('nan')], NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM (SELECT [toFloat64('nan')] AS a)
WHERE a IN (materialize([toFloat64('nan')]), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    [[toFloat64('nan')]] IN ([[toFloat64('nan')]], NULL),
    [[toFloat64('nan')]] IN (materialize([[toFloat64('nan')]]), NULL),
    [toFloat64('nan')] IN (materialize(CAST([toFloat64('nan')] AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM (SELECT [toFloat64('nan')] AS a)
WHERE a IN (materialize(CAST([toFloat64('nan')] AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_in_nan_constraint;
CREATE TABLE squality_in_nan_constraint
(
    a Array(Float64),
    b Array(Float64),
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_in_nan_constraint VALUES ([nan], [nan])
SETTINGS in_null_as_unknown = 1; -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_in_nan_constraint;
