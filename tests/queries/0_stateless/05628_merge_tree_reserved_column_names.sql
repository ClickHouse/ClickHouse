-- Tags: zookeeper

DROP TABLE IF EXISTS t_bad;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_existing;

-- A MergeTree-family table cannot get a column named like one of its virtual columns.
CREATE TABLE t_bad (x UInt32, _part_index UInt64) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_bad (x UInt32, _part String DEFAULT 'a') ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_bad (x UInt32, _table String ALIAS 'a') ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_bad (x UInt32, _part_offset UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_bad', 'r1') ORDER BY x; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_src (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_src VALUES (1);
CREATE TABLE t_bad ENGINE = MergeTree ORDER BY tuple() AS SELECT _part, x FROM t_src; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_src ADD COLUMN _part_offset UInt64; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_src ADD COLUMN y UInt32;
ALTER TABLE t_src RENAME COLUMN y TO _partition_id; -- { serverError ILLEGAL_COLUMN }

-- A table that already has such a column can still be attached and altered.
SET allow_reserved_column_names = 1;
CREATE TABLE t_existing (x UInt32, _part UInt32) ENGINE = MergeTree ORDER BY x;
SET allow_reserved_column_names = 0;
INSERT INTO t_existing VALUES (1, 10);
DETACH TABLE t_existing;
ATTACH TABLE t_existing;
ALTER TABLE t_existing ADD COLUMN y UInt32;
ALTER TABLE t_existing RENAME COLUMN _part TO part;
SELECT * FROM t_existing;

DROP TABLE t_src;
DROP TABLE t_existing;
