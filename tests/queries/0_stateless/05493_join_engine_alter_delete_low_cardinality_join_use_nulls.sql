-- ALTER DELETE rebuilds a Join table from its remaining rows; with join_use_nulls they need the same
-- Nullable conversion as an INSERT, LowCardinality columns included.

DROP TABLE IF EXISTS t_join_lc;
CREATE TABLE t_join_lc (k UInt64, s LowCardinality(String)) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO t_join_lc VALUES (1, 'a'), (2, 'b'), (3, 'c');
ALTER TABLE t_join_lc DELETE WHERE k = 2;
INSERT INTO t_join_lc VALUES (4, 'd');
SELECT k, s FROM t_join_lc ORDER BY k;
SELECT n.number, j.s FROM numbers(5) AS n ANY LEFT JOIN t_join_lc AS j ON n.number = j.k ORDER BY n.number SETTINGS join_use_nulls = 1;
DROP TABLE t_join_lc;

-- A FULL Join table stores its key; a String LowCardinality key would be stored as hashed and is not readable.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_join_lc_full;
CREATE TABLE t_join_lc_full (k LowCardinality(UInt64), v UInt64) ENGINE = Join(ALL, FULL, k) SETTINGS join_use_nulls = 1;
INSERT INTO t_join_lc_full VALUES (1, 1), (2, 2), (2, 3), (3, 4);
ALTER TABLE t_join_lc_full DELETE WHERE v = 2;
INSERT INTO t_join_lc_full VALUES (5, 5);
SELECT k, v FROM t_join_lc_full ORDER BY k, v;
SELECT k, j.v FROM (SELECT toLowCardinality(number) AS k FROM numbers(5)) AS l ALL FULL JOIN t_join_lc_full AS j USING (k) ORDER BY k, j.v SETTINGS join_use_nulls = 1;
DROP TABLE t_join_lc_full;
