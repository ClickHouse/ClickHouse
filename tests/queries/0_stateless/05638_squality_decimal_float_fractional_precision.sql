SET allow_lossy_numeric_supertype = 1;

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM VALUES('d Decimal(38, 23), x Float64', ('0.1', 0.1));

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM (SELECT toDecimal128(0.1, 23) AS d, toFloat64(0.1) AS x);

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM VALUES('d Decimal(38, 23), x Float64', (1, 1));

SELECT count() FROM VALUES('d Decimal(38, 23), x Float64', ('0.1', 0.1))
WHERE d IN (materialize(x));
