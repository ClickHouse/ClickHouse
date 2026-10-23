-- Regression tests for `optimize_push_subcolumns_into_subqueries`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_array_join_export;
DROP TABLE IF EXISTS t_push_subcolumns_json_intermediate_array;

-- An unreferenced computed export with `arrayJoin` is kept by projection pruning and reads the whole column,
-- so the subcolumn is extracted from it instead of being read a second time.
SELECT 'whole column used in an unreferenced arrayJoin export';
CREATE TABLE t_push_subcolumns_array_join_export (tup Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_array_join_export SELECT (toString(number), number % 255) FROM numbers(10);
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a)) FROM (SELECT tup, arrayJoin(range(cityHash64(tup) % 2 + 1)) AS h FROM t_push_subcolumns_array_join_export)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.a%';
SELECT
    (SELECT sum(cityHash64(tup.a)) FROM (SELECT tup, arrayJoin(range(cityHash64(tup) % 2 + 1)) AS h FROM t_push_subcolumns_array_join_export))
    = (SELECT sum(cityHash64(tup.a)) FROM (SELECT tup, arrayJoin(range(cityHash64(tup) % 2 + 1)) AS h FROM t_push_subcolumns_array_join_export)
       SETTINGS optimize_push_subcolumns_into_subqueries = 0);

-- A chain of field accesses applies element-wise to an intermediate array of objects, which a single combined
-- JSON path does not.
SELECT 'json chain over an intermediate array';
CREATE TABLE t_push_subcolumns_json_intermediate_array (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_json_intermediate_array VALUES ('{"a":[{"c":[{"e":10},{"e":20}],"d":{"e":5},"f":[[{"e":30}]]}]}');
SELECT toJSONString(x.a[1].c.e), toJSONString(x.a[1].d.e), toJSONString(x.a[1].f.e), toJSONString(x.a[1].c)
FROM (SELECT j AS x FROM t_push_subcolumns_json_intermediate_array);
SELECT toJSONString(x.a[1].c.e), toJSONString(x.a[1].d.e), toJSONString(x.a[1].f.e), toJSONString(x.a[1].c)
FROM (SELECT j AS x FROM t_push_subcolumns_json_intermediate_array)
SETTINGS optimize_push_subcolumns_into_subqueries = 0;
-- The innermost field access is still pushed.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT toJSONString(x.a[1].c.e) FROM (SELECT j AS x FROM t_push_subcolumns_json_intermediate_array)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%Array(JSON)%';

DROP TABLE t_push_subcolumns_array_join_export;
DROP TABLE t_push_subcolumns_json_intermediate_array;
