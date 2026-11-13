SET in_null_as_unknown = 1;

SELECT reinterpretAsUInt64(toFloat64(-0.)) = 9223372036854775808;
SELECT toFloat64(0) IN (toFloat64(-0.), NULL), toFloat64(0) IN (materialize(toFloat64(-0.)), NULL);
SELECT toFloat64(0) NOT IN (toFloat64(-0.), NULL), toFloat64(0) NOT IN (materialize(toFloat64(-0.)), NULL);
SELECT toFloat64(-0.) IN (toFloat64(0), NULL), toFloat64(-0.) IN (materialize(toFloat64(0)), NULL);
SELECT toFloat32(0) IN (toFloat32(-0.), NULL), toFloat32(0) IN (materialize(toFloat32(-0.)), NULL);
SELECT count() FROM values('x Float64', (0.)) WHERE x IN (toFloat64(-0.), NULL);
SELECT count() FROM values('x Float64', (0.)) WHERE x IN (materialize(toFloat64(-0.)), NULL);
SELECT toFloat64(0) IN (SELECT toFloat64(-0.));

SET in_null_as_unknown = 0;
SELECT toFloat64(0) IN (toFloat64(-0.));
SELECT toFloat64(0) NOT IN (toFloat64(-0.));
