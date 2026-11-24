SET in_null_as_unknown = 1;

SELECT map('a', '1') IN map('a', toFloat64(1)),
    map('a', '1') IN materialize(map('a', toFloat64(1)));
SELECT ['1'] IN [toFloat64(1)],
    ['1'] IN materialize([toFloat64(1)]);

SELECT count()
FROM (SELECT map('a', '1') AS m)
WHERE m IN materialize(map('a', toFloat64(1)));

SELECT
    map('a', '2000-01-01') IN map('a', toDate('2000-01-01')),
    map('a', '2000-01-01') IN materialize(map('a', toDate('2000-01-01')));

SELECT
    map('a', '10957') IN map('a', toDate('2000-01-01')),
    map('a', '10957') IN materialize(map('a', toDate('2000-01-01')));

SELECT count() FROM (SELECT map('a', '2000-01-01') AS m)
WHERE m IN map('a', toDate('2000-01-01'));

SELECT count() FROM (SELECT map('a', '2000-01-01') AS m)
WHERE m IN materialize(map('a', toDate('2000-01-01')));

SELECT
    map('a', 'a') IN map('a', toFixedString('a', 2)),
    map('a', 'a') IN materialize(map('a', toFixedString('a', 2)));

SELECT
    ['a'] IN [toFixedString('a', 2)],
    ['a'] IN materialize([toFixedString('a', 2)]);

SELECT count() FROM (SELECT map('a', 'a') AS m)
WHERE m IN map('a', toFixedString('a', 2));

SELECT count() FROM (SELECT map('a', 'a') AS m)
WHERE m IN materialize(map('a', toFixedString('a', 2)));
