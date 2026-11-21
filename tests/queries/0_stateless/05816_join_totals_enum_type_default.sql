-- The totals row of a JOIN fills an `Enum` column with the type default (its smallest value), not the column's zero,
-- when one side's totals are empty (an `arrayJoin` in them yields no row) or missing (only the other side has totals).

SET join_use_nulls = 0;
SET allow_block_nested_loop_join = 1;
-- A swapped join reads each side's totals through the other side's code path.
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS jte_l;
DROP TABLE IF EXISTS jte_r;

CREATE TABLE jte_l (id UInt8, e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
CREATE TABLE jte_r (k UInt8, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum8('a' = 1, 'b' = 2))) ENGINE = Memory;
INSERT INTO jte_l VALUES (1, 'b');
INSERT INTO jte_r VALUES (1, 'b', ('x', 'b')), (2, 'b', ('y', 'b'));

-- Each group has one row, so `z` is 5; the totals row counts 2 rows and the `arrayJoin` drops it.
-- An `INNER` join with a keyless `ON` is a cross join with a filter; an outer kind goes to the nested loop join.
SELECT 'routed';
SELECT count() FROM (EXPLAIN SELECT l.id, rr.e FROM jte_l AS l LEFT JOIN (SELECT any(e) AS e, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id < rr.z) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT count() FROM (EXPLAIN SELECT ll.e, r.id FROM (SELECT any(e) AS e, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS ll LEFT JOIN jte_l AS r ON ll.z > r.id) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT count() FROM (EXPLAIN SELECT l.e, rr.c FROM jte_l AS l LEFT JOIN (SELECT k, count() AS c FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id <= rr.k) WHERE explain LIKE '%BlockNestedLoopJoin%';

SELECT 'empty right totals';
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l INNER JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id + 4 = rr.z ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l INNER JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id + 4 = rr.z ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l INNER JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id + 4 = rr.z ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l INNER JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id + 4 = rr.z ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l INNER JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id < rr.z ORDER BY ALL;
SELECT l.id, rr.e, CAST(rr.e, 'Int8'), rr.t, rr.s FROM jte_l AS l LEFT JOIN (SELECT any(e) AS e, any(t) AS t, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id < rr.z ORDER BY ALL;

SELECT 'empty left totals';
SELECT ll.e, CAST(ll.e, 'Int8'), ll.s, r.id FROM (SELECT any(e) AS e, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS ll INNER JOIN jte_l AS r ON ll.z = r.id + 4 ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT ll.e, CAST(ll.e, 'Int8'), ll.s, r.id FROM (SELECT any(e) AS e, 'x' AS s, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS ll LEFT JOIN jte_l AS r ON ll.z > r.id ORDER BY ALL;

SELECT 'only the right side has totals';
SELECT l.e, CAST(l.e, 'Int8'), rr.c FROM jte_l AS l INNER JOIN (SELECT k, count() AS c FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id = rr.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.e, CAST(l.e, 'Int8'), rr.c FROM jte_l AS l LEFT JOIN (SELECT k, count() AS c FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id <= rr.k ORDER BY ALL;

SELECT 'join_use_nulls = 1';
SELECT l.id, rr.e FROM jte_l AS l LEFT JOIN (SELECT any(e) AS e, arrayJoin(if(count() = 1, [5], [])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id + 4 = rr.z ORDER BY ALL SETTINGS join_algorithm = 'hash', join_use_nulls = 1;

SELECT 'Nothing column';
SELECT l.id, ignore(rr.z) FROM jte_l AS l INNER JOIN (SELECT k, tuple(arrayJoin([])) AS z FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id = rr.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT ignore(l.z), rr.c FROM (SELECT id, tuple(arrayJoin([])) AS z FROM jte_l) AS l INNER JOIN (SELECT k, count() AS c FROM jte_r GROUP BY k WITH TOTALS) AS rr ON l.id = rr.k ORDER BY ALL SETTINGS join_algorithm = 'hash';

DROP TABLE jte_l;
DROP TABLE jte_r;
