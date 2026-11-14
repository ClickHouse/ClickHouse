SET in_null_as_unknown = 1;

SELECT [toInt64(0)] IN ([toFloat64(-0.6)], NULL),
       [toInt64(0)] IN (materialize([toFloat64(-0.6)]), NULL),
       [toInt64(1)] IN (materialize([toFloat64(1)]), NULL);
SELECT [toInt64(0)] IN materialize([[toFloat64(-0.6)]]),
       [toInt64(1)] IN materialize([[toFloat64(1)]]);

SELECT count() FROM values('a Array(Int64), b Array(Float64)', ([0], [-0.6]))
WHERE a IN (materialize(b), NULL);
SELECT count() FROM values('a Array(Int64), b Array(Float64)', ([0], [-0.6]))
WHERE (a IN (materialize(b), NULL)) IS NULL;

SELECT map('a', toInt64(0)) IN (materialize(map('a', toFloat64(-0.6))), NULL),
       map('a', toInt64(1)) IN (materialize(map('a', toFloat64(1))), NULL);
SELECT map('a', toInt64(0)) IN materialize([map('a', toFloat64(-0.6))]),
       map('a', toInt64(1)) IN materialize([map('a', toFloat64(1))]);

DROP TABLE IF EXISTS squality_exact_array_numeric_check;
CREATE TABLE squality_exact_array_numeric_check
(
    a Array(Int64), b Array(Float64),
    CONSTRAINT c CHECK a NOT IN (b)
) ENGINE = Memory;
INSERT INTO squality_exact_array_numeric_check VALUES ([0], [-0.6]);
SELECT a NOT IN (b) FROM squality_exact_array_numeric_check;
DROP TABLE squality_exact_array_numeric_check;

DROP TABLE IF EXISTS squality_exact_map_numeric_check;
CREATE TABLE squality_exact_map_numeric_check
(
    a Map(String, Int64), b Map(String, Float64),
    CONSTRAINT c CHECK a NOT IN (b)
) ENGINE = Memory;
INSERT INTO squality_exact_map_numeric_check VALUES (map('a', toInt64(0)), map('a', toFloat64(-0.6)));
SELECT a NOT IN (b) FROM squality_exact_map_numeric_check;
DROP TABLE squality_exact_map_numeric_check;
