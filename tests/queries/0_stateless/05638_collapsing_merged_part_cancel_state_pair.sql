-- A merged part of a CollapsingMergeTree table may keep a cancel row and a state row of one key.
-- Merges and FINAL must collapse them together with the rows of that key from newer parts.

DROP TABLE IF EXISTS t_collapsing_pair_merge;
DROP TABLE IF EXISTS t_collapsing_pair_final;

CREATE TABLE t_collapsing_pair_merge (id UInt64, sign Int8, s String) ENGINE = CollapsingMergeTree(sign) ORDER BY id;
INSERT INTO t_collapsing_pair_merge VALUES (1, -1, 'a'), (1, 1, 'b');
OPTIMIZE TABLE t_collapsing_pair_merge FINAL;
INSERT INTO t_collapsing_pair_merge VALUES (1, 1, 'c');
OPTIMIZE TABLE t_collapsing_pair_merge FINAL;
SELECT 'merge', id, sign, s FROM t_collapsing_pair_merge ORDER BY sign, s;

CREATE TABLE t_collapsing_pair_final (id UInt64, sign Int8, s String) ENGINE = CollapsingMergeTree(sign) ORDER BY id;
INSERT INTO t_collapsing_pair_final VALUES (1, -1, 'a'), (1, 1, 'b');
OPTIMIZE TABLE t_collapsing_pair_final FINAL;
SYSTEM STOP MERGES t_collapsing_pair_final;
INSERT INTO t_collapsing_pair_final VALUES (1, 1, 'c');
INSERT INTO t_collapsing_pair_final VALUES (1, -1, 'c');
SELECT 'final', count() FROM t_collapsing_pair_final FINAL;
SYSTEM START MERGES t_collapsing_pair_final;
OPTIMIZE TABLE t_collapsing_pair_final FINAL;
SELECT 'merged', count(), sum(sign) FROM t_collapsing_pair_final;

DROP TABLE t_collapsing_pair_merge;
DROP TABLE t_collapsing_pair_final;
