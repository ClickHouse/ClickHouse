SET in_null_as_unknown = 1;

SELECT
    isNull([toUInt8(0)] IN ([toInt64(-256)], [toUInt64(1)], NULL)),
    isNull([toUInt8(0)] IN (materialize([toInt64(-256)]), [toUInt64(1)], NULL));

SELECT count() FROM values('a Array(UInt8)', ([0]))
WHERE a IN (materialize([toInt64(-256)]), [toUInt64(1)], NULL);

SELECT isNull(map('a', toUInt8(0)) IN
    (materialize(map('a', toInt64(-256))), map('a', toUInt64(1)), NULL));
