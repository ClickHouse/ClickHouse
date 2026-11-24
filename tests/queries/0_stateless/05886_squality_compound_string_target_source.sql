SET in_null_as_unknown = 1;

SELECT map('a', '1') IN map('a', toFloat64(1)),
    map('a', '1') IN materialize(map('a', toFloat64(1)));
SELECT ['1'] IN [toFloat64(1)],
    ['1'] IN materialize([toFloat64(1)]);

SELECT count()
FROM (SELECT map('a', '1') AS m)
WHERE m IN materialize(map('a', toFloat64(1)));
