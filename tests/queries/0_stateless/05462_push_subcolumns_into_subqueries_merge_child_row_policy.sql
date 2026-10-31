-- A row policy of a table read through a `Merge` table is attached when `Merge` builds the read of its
-- child, after the pushdown decision. When the policy reads the whole column, the column stays read,
-- so the subcolumn is extracted from it instead of being read a second time.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP ROW POLICY IF EXISTS rp_05462 ON t_policy_05462;
DROP TABLE IF EXISTS t_policy_05462;

CREATE TABLE t_policy_05462 (tup Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_policy_05462 SELECT (repeat(hex(cityHash64(number)), 12), number % 255) FROM numbers(10000);

SELECT 'without a row policy';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM merge(currentDatabase(), '^t_policy_05462$')))
WHERE explain LIKE '%tup.a%';

CREATE ROW POLICY rp_05462 ON t_policy_05462 USING cityHash64(tup) % 2 = 0 TO ALL;

SELECT 'row policy of the child reads the whole column';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM merge(currentDatabase(), '^t_policy_05462$')))
WHERE explain LIKE '%tup.a%';
SELECT
    (SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM merge(currentDatabase(), '^t_policy_05462$')))
    = (SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM merge(currentDatabase(), '^t_policy_05462$')) SETTINGS optimize_push_subcolumns_into_subqueries = 0);

DROP ROW POLICY rp_05462 ON t_policy_05462;
DROP TABLE t_policy_05462;
