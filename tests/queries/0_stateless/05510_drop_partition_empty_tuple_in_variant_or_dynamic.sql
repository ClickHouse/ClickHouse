-- A table without a partition key has one partition, `tuple()`. The partition expression is a constant, which may be
-- carried by a `Variant` or a `Dynamic`; their active value is what counts.

DROP TABLE IF EXISTS t_no_partition_key;
CREATE TABLE t_no_partition_key (x UInt8) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_no_partition_key VALUES (1);
ALTER TABLE t_no_partition_key DROP PARTITION tuple();
SELECT count() FROM t_no_partition_key;

INSERT INTO t_no_partition_key VALUES (2);
ALTER TABLE t_no_partition_key DROP PARTITION CAST(tuple() AS Dynamic);
SELECT count() FROM t_no_partition_key;

INSERT INTO t_no_partition_key VALUES (3);
ALTER TABLE t_no_partition_key DROP PARTITION CAST(tuple() AS Variant(Tuple(), UInt64));
SELECT count() FROM t_no_partition_key;

ALTER TABLE t_no_partition_key DROP PARTITION CAST(1 AS Dynamic); -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_no_partition_key DROP PARTITION CAST(1 AS Variant(Tuple(), UInt8)); -- { serverError INVALID_PARTITION_VALUE }

DROP TABLE t_no_partition_key;
