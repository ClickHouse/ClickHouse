CREATE TABLE t_hypothesis_sanity (x UInt8, INDEX i x TYPE hypothesis GRANULARITY 100) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_hypothesis_sanity VALUES (1), (2);
SELECT 1 FROM t_hypothesis_sanity WHERE x = 1;

CREATE TABLE t_hypothesis_join (c0 Int, INDEX i0 c0 TYPE hypothesis GRANULARITY 9) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_hypothesis_join (c0) VALUES (1), (2), (3), (2), (4), (5), (6), (7);
SELECT 1 FROM t_hypothesis_join tx JOIN t_hypothesis_join ON tx.c0 = t_hypothesis_join.c0;

DROP TABLE t_hypothesis_sanity;
DROP TABLE t_hypothesis_join;
