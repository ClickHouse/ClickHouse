-- A `JSON` constant converted to a `String` renders as `CAST` renders it: the dynamic paths keep their
-- inferred types, so a `Date` is not written as a number.
SELECT 'top-level JSON';
SELECT CAST(CAST('{"d":"2020-01-01"}', 'JSON'), 'String');
SELECT x FROM values('x String', CAST('{"d":"2020-01-01"}', 'JSON'));
SELECT '{"d":"2020-01-01"}' IN (CAST('{"d":"2020-01-01"}', 'JSON')),
       '{"d":18262}' IN (CAST('{"d":"2020-01-01"}', 'JSON'));

DROP TABLE IF EXISTS t_json_set;
CREATE TABLE t_json_set (s String) ENGINE = Memory;
INSERT INTO t_json_set VALUES ('{"d":"2020-01-01"}'), ('{"d":18262}');
SELECT s FROM t_json_set WHERE s IN (CAST('{"d":"2020-01-01"}', 'JSON'));
SELECT s FROM t_json_set WHERE s IN (CAST(CAST('{"d":"2020-01-01"}', 'JSON'), 'String'));
DROP TABLE t_json_set;

-- `JSON` inside a composite, converted to a `String` as a whole or member by member.
SELECT 'JSON in composites';
SELECT x FROM values('x String', CAST(['{"d":"2020-01-01"}'], 'Array(JSON)'));
SELECT x FROM values('x Array(String)', CAST(['{"d":"2020-01-01"}'], 'Array(JSON)'));
SELECT x FROM values('x Tuple(String)', tuple(CAST('{"d":"2020-01-01"}', 'JSON')));
SELECT x FROM values('x Map(String, String)', CAST(map('k', '{"d":"2020-01-01"}'), 'Map(String, JSON)'));
SELECT ['{"d":"2020-01-01"}'] IN (CAST(['{"d":"2020-01-01"}'], 'Array(JSON)'));
SELECT tuple('{"d":"2020-01-01"}') IN (tuple(CAST('{"d":"2020-01-01"}', 'JSON')));
SELECT map('k', '{"d":"2020-01-01"}') IN (CAST(map('k', '{"d":"2020-01-01"}'), 'Map(String, JSON)'));

-- `JSON` held by a `Dynamic`.
SELECT 'JSON in Dynamic';
SELECT x FROM values('x String', CAST(CAST('{"d":"2020-01-01"}', 'JSON'), 'Dynamic'));
SELECT '{"d":"2020-01-01"}' IN (CAST(CAST('{"d":"2020-01-01"}', 'JSON'), 'Dynamic'));
