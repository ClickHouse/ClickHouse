SET in_null_as_unknown = 1;

SELECT [tuple(toInt64(0))] IN ([tuple(toFloat64(-0.6))], NULL),
       [tuple(toInt64(0))] IN (materialize([tuple(toFloat64(-0.6))]), NULL),
       [tuple(toInt64(0))] NOT IN (materialize([tuple(toFloat64(-0.6))]), NULL);
SELECT [tuple(toInt64(0))] IN materialize([[tuple(toFloat64(-0.6))]]),
       [tuple(toInt64(1))] IN materialize([[tuple(toFloat64(1))]]);

SELECT count()
FROM (SELECT [tuple(toInt64(0))] AS a, [tuple(toFloat64(-0.6))] AS b)
WHERE a IN (materialize(b), NULL);
SELECT count()
FROM (SELECT [tuple(toInt64(0))] AS a, [tuple(toFloat64(-0.6))] AS b)
WHERE (a IN (materialize(b), NULL)) IS NULL;

DROP TABLE IF EXISTS squality_nested_tuple_numeric_check;
CREATE TABLE squality_nested_tuple_numeric_check
(
    a Array(Tuple(Int64)), b Array(Tuple(Float64)),
    CONSTRAINT c CHECK a NOT IN materialize(b)
) ENGINE = Memory;
INSERT INTO squality_nested_tuple_numeric_check SELECT [tuple(toInt64(0))], [tuple(toFloat64(-0.6))];
SELECT a NOT IN materialize(b) FROM squality_nested_tuple_numeric_check;
DROP TABLE squality_nested_tuple_numeric_check;
