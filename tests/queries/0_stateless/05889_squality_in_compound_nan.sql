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
