-- JOIN -> IN moves the right side of the join into the source of a new set, which index analysis can
-- build synchronously, before the outer plan materializes its CTEs and builds its sets. The source
-- declares what it reads instead, so the join still converts. Each query runs with the conversion
-- off and on (the pairs must match), with and without sets built for index analysis, and as a
-- mutation of a Memory table, which runs in the query thread with these settings.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET query_plan_enable_optimizations = 1;
SET join_algorithm = 'hash';
SET optimize_move_to_prewhere = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS keys;
DROP TABLE IF EXISTS rhs;
DROP TABLE IF EXISTS rhs_mt;
DROP TABLE IF EXISTS target;

CREATE TABLE keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO keys SELECT number FROM numbers(4);
CREATE TABLE rhs (k UInt64) ENGINE = Memory;
INSERT INTO rhs SELECT number FROM numbers(4);
CREATE TABLE rhs_mt (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rhs_mt SELECT number FROM numbers(4);
CREATE TABLE target (k UInt64, v UInt8) ENGINE = Memory;
INSERT INTO target SELECT number, 0 FROM numbers(4);

-- A CTE that reads another CTE: the outer one must wait for the inner one.
SELECT 'two-level CTE';
SELECT DISTINCT l.k FROM keys AS l ALL INNER JOIN
(
    WITH c1 AS MATERIALIZED (SELECT number AS k FROM numbers(3)),
         c2 AS MATERIALIZED (SELECT k FROM c1 WHERE k < 2 UNION ALL SELECT k FROM c1 WHERE k < 2)
    SELECT k FROM c2 UNION ALL SELECT k FROM c2
) AS r ON l.k = r.k ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0;
SELECT DISTINCT l.k FROM keys AS l ALL INNER JOIN
(
    WITH c1 AS MATERIALIZED (SELECT number AS k FROM numbers(3)),
         c2 AS MATERIALIZED (SELECT k FROM c1 WHERE k < 2 UNION ALL SELECT k FROM c1 WHERE k < 2)
    SELECT k FROM c2 UNION ALL SELECT k FROM c2
) AS r ON l.k = r.k ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT countIf(trimLeft(explain) LIKE 'Join%') FROM (EXPLAIN description = 0
    SELECT l.k FROM keys AS l ALL INNER JOIN
    (
        WITH c1 AS MATERIALIZED (SELECT number AS k FROM numbers(3)),
             c2 AS MATERIALIZED (SELECT k FROM c1 WHERE k < 2 UNION ALL SELECT k FROM c1 WHERE k < 2)
        SELECT k FROM c2 UNION ALL SELECT k FROM c2
    ) AS r ON l.k = r.k) SETTINGS query_plan_convert_join_to_in = 1;

-- A CTE read through a filtered MergeTree right side.
SELECT 'CTE behind a MergeTree read';
SELECT DISTINCT l.k FROM keys AS l ALL INNER JOIN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT k FROM rhs_mt WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
) AS r ON l.k = r.k ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0, use_index_for_in_with_subqueries = 0;
SELECT DISTINCT l.k FROM keys AS l ALL INNER JOIN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT k FROM rhs_mt WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
) AS r ON l.k = r.k ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1, use_index_for_in_with_subqueries = 0;
SELECT DISTINCT l.k FROM keys AS l ALL INNER JOIN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT k FROM rhs_mt WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
) AS r ON l.k = r.k ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1, use_index_for_in_with_subqueries = 1;

-- An IN subquery in the ON key that reads a CTE.
SELECT 'IN in the key, reading a CTE';
WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT k FROM c UNION ALL SELECT k FROM c))
ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0;
WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT k FROM c UNION ALL SELECT k FROM c))
ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT countIf(trimLeft(explain) LIKE 'Join%') FROM (EXPLAIN description = 0
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT k FROM c UNION ALL SELECT k FROM c)))
SETTINGS query_plan_convert_join_to_in = 1;

-- An IN filter in ON on the right side only, the shape of 02841.
SELECT 'IN filter in ON';
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k AND b.k IN (SELECT number FROM numbers(2))
ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0;
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k AND b.k IN (SELECT number FROM numbers(2))
ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1, use_index_for_in_with_subqueries = 0;
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k AND b.k IN (SELECT number FROM numbers(2))
ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1, use_index_for_in_with_subqueries = 1;
SELECT countIf(trimLeft(explain) LIKE 'Join%') FROM (EXPLAIN description = 0
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k AND b.k IN (SELECT number FROM numbers(2)))
SETTINGS query_plan_convert_join_to_in = 1;

-- The set in the key is also read outside the join: by another UNION ALL branch and by a left filter.
SELECT 'IN in the key, shared';
SELECT DISTINCT * FROM
(
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2)))
    UNION ALL SELECT k + 10 FROM rhs WHERE k IN (SELECT number FROM numbers(2))
) ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0;
SELECT DISTINCT * FROM
(
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2)))
    UNION ALL SELECT k + 10 FROM rhs WHERE k IN (SELECT number FROM numbers(2))
) ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2)))
WHERE a.k IN (SELECT number FROM numbers(2)) OR a.k = 3 ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 0;
SELECT DISTINCT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT number FROM numbers(2)))
WHERE a.k IN (SELECT number FROM numbers(2)) OR a.k = 3 ORDER BY 1 SETTINGS query_plan_convert_join_to_in = 1;

-- The same shapes in a mutation.
SELECT 'mutations';
ALTER TABLE target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k + toUInt64(b.k IN (SELECT k FROM c UNION ALL SELECT k FROM c))
) WHERE 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT groupArray(v) FROM (SELECT v FROM target ORDER BY k);
ALTER TABLE target UPDATE v = k IN
(
    SELECT a.k FROM keys AS a ALL INNER JOIN rhs AS b ON a.k = b.k AND b.k IN (SELECT number FROM numbers(2))
) WHERE 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT groupArray(v) FROM (SELECT v FROM target ORDER BY k);
ALTER TABLE target UPDATE v = k IN
(
    SELECT l.k FROM keys AS l ALL INNER JOIN
    (
        WITH c1 AS MATERIALIZED (SELECT number AS k FROM numbers(3)),
             c2 AS MATERIALIZED (SELECT k FROM c1 WHERE k < 2 UNION ALL SELECT k FROM c1 WHERE k < 2)
        SELECT k FROM c2 UNION ALL SELECT k FROM c2
    ) AS r ON l.k = r.k
) WHERE 1 SETTINGS query_plan_convert_join_to_in = 1;
SELECT groupArray(v) FROM (SELECT v FROM target ORDER BY k);

DROP TABLE keys;
DROP TABLE rhs;
DROP TABLE rhs_mt;
DROP TABLE target;

-- A GLOBAL IN filter pushed into the right side, the shape of 04092: its set gets an external table
-- later, so the new set source cannot build it and the join stays.
SELECT 'GLOBAL IN pushed into the right side';
DROP TABLE IF EXISTS local_table;
DROP TABLE IF EXISTS dist_table;
DROP TABLE IF EXISTS merge_table;
DROP TABLE IF EXISTS second_local;
DROP TABLE IF EXISTS second_dist;

CREATE TABLE local_table (id String, id2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_table (id String, id2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_table');
CREATE TABLE merge_table (id String, id2 String) ENGINE = Merge(currentDatabase(), '^(dist_table)$');
CREATE TABLE second_local (id String, id2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE second_dist (id String, id2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'second_local');
INSERT INTO local_table VALUES ('a', 'x'), ('b', 'y');
INSERT INTO second_local VALUES ('a', 'x'), ('c', 'z');

SELECT count() FROM merge_table AS s GLOBAL ANY INNER JOIN second_dist AS f USING (id)
WHERE f.id2 GLOBAL IN (SELECT id2 FROM second_dist GROUP BY id2) SETTINGS query_plan_convert_join_to_in = 0;
SELECT count() FROM merge_table AS s GLOBAL ANY INNER JOIN second_dist AS f USING (id)
WHERE f.id2 GLOBAL IN (SELECT id2 FROM second_dist GROUP BY id2) SETTINGS query_plan_convert_join_to_in = 1;

-- Readers that evaluate their filter while planning (`merge` selecting tables, `system.tables`
-- filtering databases) need the new set at once, which a GLOBAL IN in the key rules out: the join stays.
SELECT 'GLOBAL IN in the key, planning-time readers';
CREATE TABLE r_keys (k UInt64) ENGINE = Memory;
INSERT INTO r_keys VALUES (0), (1), (2);
SELECT count() FROM system.tables AS l ANY INNER JOIN r_keys AS r
ON l.database = if(r.k GLOBAL IN (SELECT number FROM numbers(2)), currentDatabase(), 'bad')
SETTINGS query_plan_convert_join_to_in = 0;
SELECT count() FROM system.tables AS l ANY INNER JOIN r_keys AS r
ON l.database = if(r.k GLOBAL IN (SELECT number FROM numbers(2)), currentDatabase(), 'bad')
SETTINGS query_plan_convert_join_to_in = 1;
SELECT count() FROM merge(currentDatabase(), '^local_table$') AS l ANY INNER JOIN r_keys AS r
ON l._table = if(r.k GLOBAL IN (SELECT number FROM numbers(2)), 'local_table', 'bad')
SETTINGS query_plan_convert_join_to_in = 0;
SELECT count() FROM merge(currentDatabase(), '^local_table$') AS l ANY INNER JOIN r_keys AS r
ON l._table = if(r.k GLOBAL IN (SELECT number FROM numbers(2)), 'local_table', 'bad')
SETTINGS query_plan_convert_join_to_in = 1;
DROP TABLE r_keys;

DROP TABLE merge_table;
DROP TABLE dist_table;
DROP TABLE local_table;
DROP TABLE second_dist;
DROP TABLE second_local;
