-- the smallest column is an ALIAS of a constant, so a projection that only counts rows needs no column of the table
CREATE TABLE t (s String, a UInt8 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES ('x'), ('y');

SELECT count() FROM t SETTINGS optimize_trivial_count_query = 0;
SELECT s, count() FROM t GROUP BY s ORDER BY s;
SELECT a, count() FROM t GROUP BY a;

-- the same for a projection of the user that the parts do not have
ALTER TABLE t ADD PROJECTION p (SELECT a, count() GROUP BY a);

SELECT count() FROM t SETTINGS optimize_trivial_count_query = 0;
SELECT a, count() FROM t GROUP BY a;
SELECT count() FROM t WHERE s = 'x';
