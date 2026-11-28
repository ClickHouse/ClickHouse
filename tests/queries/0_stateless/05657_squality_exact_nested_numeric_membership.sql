SET in_null_as_unknown = 1;

SELECT [[toInt64(0)]] IN (materialize([[toFloat64(-0.6)]]), NULL),
       [[toInt64(1)]] IN (materialize([[toFloat64(1)]]), NULL);
SELECT [[toInt64(0)]] IN materialize([[[toFloat64(-0.6)]]]),
       [[toInt64(1)]] IN materialize([[[toFloat64(1)]]]);

SELECT map(toInt64(0), toUInt8(1)) IN (materialize(map(toFloat64(-0.6), toUInt8(1))), NULL),
       map(toInt64(1), toUInt8(1)) IN (materialize(map(toFloat64(1), toUInt8(1))), NULL);
SELECT map(toInt64(0), toUInt8(1)) IN materialize([map(toFloat64(-0.6), toUInt8(1))]),
       map(toInt64(1), toUInt8(1)) IN materialize([map(toFloat64(1), toUInt8(1))]);
