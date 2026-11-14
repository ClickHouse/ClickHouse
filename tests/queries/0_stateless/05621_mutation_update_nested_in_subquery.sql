-- An IN subquery nested in another one inside an UPDATE expression made the mutation hang. The update
-- expressions were resolved a second time, which registered a second copy of each set; both copies
-- of the inner set got the same key, and in the mutation pipeline the second one waited forever on
-- the first through the shared per-mutation set cache.

DROP TABLE IF EXISTS t_nested_in;
DROP TABLE IF EXISTS r_nested_in;

CREATE TABLE t_nested_in (k UInt64, v UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r_nested_in (k UInt64) ENGINE = Memory;
INSERT INTO t_nested_in SELECT number, 0 FROM numbers(4);
INSERT INTO r_nested_in SELECT number FROM numbers(4);

ALTER TABLE t_nested_in UPDATE v = k IN (SELECT k FROM r_nested_in WHERE k IN (SELECT number FROM numbers(2))) WHERE 1
SETTINGS mutations_sync = 2;
SELECT groupArray(v) FROM (SELECT v FROM t_nested_in ORDER BY k);

-- The same nested subquery in WHERE as well.
ALTER TABLE t_nested_in UPDATE v = k IN (SELECT k FROM r_nested_in WHERE k IN (SELECT number FROM numbers(3)))
WHERE k IN (SELECT k FROM r_nested_in WHERE k IN (SELECT number FROM numbers(3)))
SETTINGS mutations_sync = 2;
SELECT groupArray(v) FROM (SELECT v FROM t_nested_in ORDER BY k);

DROP TABLE t_nested_in;
DROP TABLE r_nested_in;
