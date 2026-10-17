-- Tags: no-fasttest
-- no-fasttest: collations require ICU

-- Read-in-order virtual rows cover only a prefix of the sort description. With per-block virtual
-- rows there is no preliminary merge, so a downstream merge whose sort description contains a
-- `COLLATE` must compare a virtual row only on its covered prefix, as the non-collation merge does.

DROP TABLE IF EXISTS t_virtual_row_collation;

CREATE TABLE t_virtual_row_collation (a UInt32, s String)
ENGINE = MergeTree ORDER BY (a, s)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_collation;

INSERT INTO t_virtual_row_collation SELECT number % 10, toString(number % 7) FROM numbers(2000);
INSERT INTO t_virtual_row_collation SELECT number % 10, toString(number % 7) FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_collation SELECT number % 10, toString(number % 7) FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_collation SELECT number % 10, toString(number % 7) FROM numbers(6000, 2000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
    optimize_distinct_in_order = 1, read_in_order_two_level_merge_threshold = 1, max_threads = 2, max_block_size = 64;
SET allow_distinct_partitions_independently = 0, force_distinct_partitions_independently = 0;

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_collation ORDER BY a DESC, s DESC COLLATE 'en'))
  = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_collation ORDER BY a DESC, s DESC COLLATE 'en' SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT DISTINCT a, s FROM t_virtual_row_collation ORDER BY a, s DESC COLLATE 'en'))
  = (SELECT groupArray((a, s)) FROM (SELECT DISTINCT a, s FROM t_virtual_row_collation ORDER BY a, s DESC COLLATE 'en' SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT DISTINCT a, s FROM t_virtual_row_collation ORDER BY 'x', a DESC, 'd' COLLATE 'cs', s DESC))
  = (SELECT groupArray((a, s)) FROM (SELECT DISTINCT a, s FROM t_virtual_row_collation ORDER BY 'x', a DESC, 'd' COLLATE 'cs', s DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_collation ORDER BY a, 'd' COLLATE 'cs', s))
  = (SELECT groupArray((a, s)) FROM (SELECT a, s FROM t_virtual_row_collation ORDER BY a, 'd' COLLATE 'cs', s SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_collation;
