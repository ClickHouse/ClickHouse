-- With `part_minmax_index_columns = 'with_block_number_offset'`, the min-max index of a part covers `_block_number` and
-- `_block_offset`. The versions of a row that `FINAL` merges have different values of them, so pruning parts by them
-- dropped versions the merge needs: https://github.com/ClickHouse/ClickHouse/issues/125442

-- The block numbers are taken from the data: they start at 1 and are counted per table in `MergeTree`, while `SharedMergeTree`
-- starts them at 0, and `ReplicatedMergeTree` also counts them per partition. So the first insert into `p = 2` may have the same
-- block number as the first insert into `p = 1`, and its row is replaced by the second insert, which `FINAL` keeps in any case.

DROP TABLE IF EXISTS t_replacing;
DROP TABLE IF EXISTS t_summing;
DROP TABLE IF EXISTS t_collapsing;

CREATE TABLE t_replacing (p UInt8, k UInt64, v String) ENGINE = ReplacingMergeTree PARTITION BY p ORDER BY (p, k)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';
SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (1, 1, 'old'), (1, 2, 'x');
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (1, 1, 'new');
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (2, 1, 'y_old');
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (2, 1, 'y');

SELECT 'replacing';
SELECT k, v FROM t_replacing FINAL WHERE _block_number = (SELECT min(_block_number) FROM t_replacing WHERE p = 1) ORDER BY k;
SELECT count() FROM t_replacing FINAL WHERE _block_number = (SELECT min(_block_number) FROM t_replacing WHERE p = 1);
SELECT k, v FROM t_replacing FINAL WHERE p = 1 AND _block_number = (SELECT min(_block_number) FROM t_replacing WHERE p = 1) ORDER BY k;
SELECT k, v FROM t_replacing FINAL WHERE p = 2 ORDER BY k;
SELECT k, v FROM t_replacing WHERE p = 1 AND _block_number = (SELECT min(_block_number) FROM t_replacing WHERE p = 1) ORDER BY k;

CREATE TABLE t_summing (k UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing SETTINGS optimize_on_insert = 0 VALUES (1, 10);
INSERT INTO t_summing SETTINGS optimize_on_insert = 0 VALUES (1, 5);

SELECT 'summing';
SELECT k, s FROM t_summing FINAL WHERE _block_number = (SELECT min(_block_number) FROM t_summing);

CREATE TABLE t_collapsing (k UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';
SYSTEM STOP MERGES t_collapsing;
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 VALUES (1, 1);
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 VALUES (1, -1);

SELECT 'collapsing';
SELECT k, sign FROM t_collapsing FINAL WHERE _block_number = (SELECT min(_block_number) FROM t_collapsing);

DROP TABLE t_replacing;
DROP TABLE t_summing;
DROP TABLE t_collapsing;
