-- The right side of a `LATERAL JOIN` reads the left side of that `JOIN`, so under
-- `analyzer_alias_hides_table_name` a table expression there hides the name of an aliased inner table.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS t_lateral_qualifier;
CREATE TABLE t_lateral_qualifier (id UInt64, grp UInt8, val UInt64) ENGINE = Memory;
INSERT INTO t_lateral_qualifier VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30);

SELECT 'hidden by a left side with several tables';
-- The name refers to the outer `t_lateral_qualifier`, so this is the same query as the next one with the outer
-- table aliased. A correlated column of a `LATERAL JOIN` over a nested `JOIN` is not supported by the planner yet.
SELECT t_lateral_qualifier.id, s.cnt
FROM t_lateral_qualifier INNER JOIN t_lateral_qualifier AS o ON t_lateral_qualifier.id = o.id
INNER JOIN LATERAL (
    SELECT count() AS cnt FROM t_lateral_qualifier AS c
    WHERE c.grp = o.grp AND c.val > t_lateral_qualifier.val
) AS s ON true
ORDER BY t_lateral_qualifier.id
SETTINGS analyzer_alias_hides_table_name = 1; -- { serverError NOT_IMPLEMENTED }
SELECT o2.id, s.cnt
FROM t_lateral_qualifier AS o2 INNER JOIN t_lateral_qualifier AS o ON o2.id = o.id
INNER JOIN LATERAL (
    SELECT count() AS cnt FROM t_lateral_qualifier AS c
    WHERE c.grp = o.grp AND c.val > o2.val
) AS s ON true
ORDER BY o2.id; -- { serverError NOT_IMPLEMENTED }

SELECT 'the only correlated reference';
SELECT t_lateral_qualifier.id, s.cnt
FROM t_lateral_qualifier
INNER JOIN LATERAL (
    SELECT count() AS cnt FROM t_lateral_qualifier AS c WHERE c.val > t_lateral_qualifier.val
) AS s ON true
ORDER BY t_lateral_qualifier.id
SETTINGS analyzer_alias_hides_table_name = 1;

SELECT 'old resolution without the setting';
SELECT o.id, s.cnt
FROM t_lateral_qualifier AS o
INNER JOIN LATERAL (
    SELECT count() AS cnt FROM t_lateral_qualifier AS c WHERE c.grp = o.grp AND c.val > t_lateral_qualifier.val
) AS s ON true
ORDER BY o.id;

DROP TABLE t_lateral_qualifier;
