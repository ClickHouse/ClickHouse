SET in_null_as_unknown = 1;

SELECT 'a' IN toFixedString('a', 2), 'a' IN materialize(toFixedString('a', 2));

SELECT count() FROM values('x String', ('a')) WHERE x IN toFixedString('a', 2);
SELECT count() FROM values('x String', ('a')) WHERE x IN materialize(toFixedString('a', 2));

SELECT 'a' IN (SELECT toFixedString('a', 2)); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM values('x String', ('a'))
WHERE x IN (SELECT toFixedString('a', 2)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_fixed_string_set;
CREATE TABLE squality_prepared_fixed_string_set (k FixedString(2)) ENGINE = Set;
INSERT INTO squality_prepared_fixed_string_set SELECT toFixedString('a', 2);
SELECT 'a' IN squality_prepared_fixed_string_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_fixed_string_set;
