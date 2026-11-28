-- Tags: no-parallel-replicas
-- no-parallel-replicas: the rewrite is skipped with parallel replicas

DROP TABLE IF EXISTS t_aj_chain;

CREATE TABLE t_aj_chain (id UInt64, name String, a Array(UInt32), b Array(UInt32), c Array(UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_aj_chain
SELECT number, toString(sipHash64(number)), range(number % 3), range(number % 4), range(number % 2)
FROM numbers(1000);

SET optimize_read_in_order = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET query_plan_top_k_through_array_join = 1;

SELECT '-- two LEFT ARRAY JOIN: the sort goes below both';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]*', '')) FROM (EXPLAIN SELECT id, x, y FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY name LIMIT 31)
WHERE explain LIKE '%ARRAY JOIN %' OR explain LIKE '%Sorting (%';

SELECT '-- three LEFT ARRAY JOIN';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]*', '')) FROM (EXPLAIN SELECT id, x, y, z FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN c AS z ORDER BY name LIMIT 31)
WHERE explain LIKE '%ARRAY JOIN %' OR explain LIKE '%Sorting (%';

SELECT '-- the sort stops above the ARRAY JOIN that makes its key';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]*', '')) FROM (EXPLAIN SELECT id, x, y, z FROM t_aj_chain ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN c AS z ORDER BY x, name LIMIT 31)
WHERE explain LIKE '%ARRAY JOIN %' OR explain LIKE '%Sorting (%';

SELECT '-- same keys as without the rewrite';
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN c AS z ORDER BY name DESC LIMIT 31 OFFSET 7))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN c AS z ORDER BY name DESC LIMIT 31 OFFSET 7 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x ARRAY JOIN b AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x ARRAY JOIN b AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x ARRAY JOIN b AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x ARRAY JOIN b AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray((x, name)) FROM (SELECT x, name FROM t_aj_chain ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY x, name LIMIT 31))
    = (SELECT groupArray((x, name)) FROM (SELECT x, name FROM t_aj_chain ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y ORDER BY x, name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN range(x) AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN range(x) AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x ARRAY JOIN range(x) AS y ORDER BY name LIMIT 31))
    = (SELECT groupArray(name) FROM (SELECT name FROM t_aj_chain ARRAY JOIN a AS x ARRAY JOIN range(x) AS y ORDER BY name LIMIT 31 SETTINGS query_plan_top_k_through_array_join = 0));

DROP TABLE t_aj_chain;
