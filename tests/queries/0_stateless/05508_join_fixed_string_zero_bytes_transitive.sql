-- `FixedString = String` ignores trailing zero bytes while `String = String` does not, so such a predicate
-- must not take part in the transitive equivalences of the join reorderer: it must neither make an explicit
-- `String = String` predicate redundant nor be used to synthesize one.

SET enable_join_transitive_predicates = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS fs;
DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS s2;

CREATE TABLE fs (x FixedString(3)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE s1 (y String) ENGINE = MergeTree ORDER BY y;
CREATE TABLE s2 (z String) ENGINE = MergeTree ORDER BY z;

INSERT INTO fs VALUES ('a');
INSERT INTO s1 SELECT 'a' FROM numbers(100);
INSERT INTO s2 SELECT 'a\0\0' FROM numbers(100);

SELECT '-- x matches both y and z, y does not match z';
SELECT count() FROM fs JOIN s1 ON x = y JOIN s2 ON x = z;
SELECT count() FROM s1 JOIN s2 ON y = z;

SELECT '-- the explicit y = z must not be removed as redundant';
SELECT count() FROM fs JOIN s1 ON x = y JOIN s2 ON x = z AND y = z;
SELECT count() FROM fs JOIN s1 ON x = y JOIN s2 ON x = z AND y = z SETTINGS enable_join_transitive_predicates = 0;
SELECT count() FROM fs, s1, s2 WHERE x = y AND x = z AND y = z;

TRUNCATE TABLE fs;
TRUNCATE TABLE s1;
TRUNCATE TABLE s2;

INSERT INTO fs SELECT 'a' FROM numbers(10000);
INSERT INTO s1 VALUES ('a');
INSERT INTO s2 VALUES ('a\0\0');

SELECT '-- y = z must not be synthesized when the two small tables are joined first';
SELECT count() FROM fs JOIN s1 ON x = y JOIN s2 ON x = z;
SELECT count() FROM fs JOIN s1 ON x = y JOIN s2 ON x = z SETTINGS enable_join_transitive_predicates = 0;
SELECT count() FROM fs, s1, s2 WHERE x = y AND x = z;

SELECT '-- the same with LowCardinality and Nullable wrappers';
SELECT count() FROM fs JOIN (SELECT toLowCardinality(y) AS y FROM s1) AS s1 ON x = y JOIN (SELECT toNullable(z) AS z FROM s2) AS s2 ON x = z;
SELECT count() FROM fs JOIN (SELECT toLowCardinality(y) AS y FROM s1) AS s1 ON x = y JOIN (SELECT toNullable(z) AS z FROM s2) AS s2 ON x = z AND y = z;

DROP TABLE fs;
DROP TABLE s1;
DROP TABLE s2;
