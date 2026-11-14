SET in_null_as_unknown = 1;

SELECT toInt64(0) IN (-0.6, NULL), toInt64(0) NOT IN (-0.6, NULL);
SELECT toInt64(0) IN (materialize(-0.6), NULL), toInt64(0) NOT IN (materialize(-0.6), NULL);

SELECT toInt64(0) IN (materialize(-0.6), toInt64(0), NULL),
       toInt64(0) NOT IN (materialize(-0.6), toInt64(0), NULL);

SELECT count() FROM VALUES('x Int64', (0))
WHERE x IN (materialize(-0.6), NULL);

SELECT toNullable(number) IN (materialize(-0.6), if(number = 0, NULL, toFloat64(1)))
FROM numbers(2) ORDER BY number;
