-- Chained JSON-array access over a column exported by a subquery must keep the values of every
-- runtime alternative of the `Dynamic` path, not only those stored as `Array(JSON)`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_json_array_alternatives;
CREATE TABLE t_json_array_alternatives (id UInt32, i UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_array_alternatives VALUES
    (1, 2, '{"a":[null,{"b":42}]}'),
    (2, 4, '{"a":[42,"hello",[1,2,3],{"b":42}]}'),
    (3, 1, '{"a":[[{"b":42}]]}'),
    (4, 2, '{"a":[{"b":1},{"b":2}]}');

SELECT id, toJSONString(x.a[i].b)
FROM (SELECT id, i, j AS x FROM t_json_array_alternatives)
ORDER BY id;

SELECT id, toJSONString(x[i].b)
FROM (SELECT id, i, j.a AS x FROM t_json_array_alternatives)
ORDER BY id;

-- The access is not rewritten into the typed `Array(JSON)` subcolumn, and an array index that
-- reads the same path does not add another read next to `j.a`.
SELECT sum(cityHash64(x.a[length(toJSONString(x.a)) % 2 + 1].b))
    = (SELECT sum(cityHash64(x.a[length(toJSONString(x.a)) % 2 + 1].b)) FROM (SELECT j AS x FROM t_json_array_alternatives WHERE id = 4) SETTINGS optimize_push_subcolumns_into_subqueries = 0)
FROM (SELECT j AS x FROM t_json_array_alternatives WHERE id = 4);

SELECT count() FROM (EXPLAIN QUERY TREE SELECT x.a[length(toJSONString(x.a)) % 2 + 1].b FROM (SELECT j AS x FROM t_json_array_alternatives WHERE id = 4))
WHERE explain LIKE '%Array(JSON)%';

DROP TABLE t_json_array_alternatives;
