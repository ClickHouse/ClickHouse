SET in_null_as_unknown = 1;

SELECT
    CAST([(1, 2)] AS Array(Tuple(x UInt8, y UInt8))) IN CAST([(2, 1)] AS Array(Tuple(y UInt8, x UInt8))),
    CAST([(1, 2)] AS Array(Tuple(x UInt8, y UInt8))) IN materialize(CAST([(2, 1)] AS Array(Tuple(y UInt8, x UInt8))));

SELECT count() FROM values('x UInt8', (1))
WHERE CAST([(x, 2)] AS Array(Tuple(x UInt8, y UInt8))) IN
    materialize(CAST([(2, 1)] AS Array(Tuple(y UInt8, x UInt8))));

SELECT CAST([(1, 2)] AS Array(Tuple(x UInt8, y UInt8))) IN
    (SELECT CAST([(2, 1)] AS Array(Tuple(y UInt8, x UInt8)))); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_named_tuple_set;
CREATE TABLE squality_prepared_named_tuple_set (k Array(Tuple(y UInt8, x UInt8))) ENGINE = Set;
INSERT INTO squality_prepared_named_tuple_set SELECT CAST([(2, 1)] AS Array(Tuple(y UInt8, x UInt8)));
SELECT CAST([(1, 2)] AS Array(Tuple(x UInt8, y UInt8))) IN squality_prepared_named_tuple_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_named_tuple_set;
