-- Tags: zookeeper
-- zookeeper: the second table is ReplicatedMergeTree.

-- A partition value that does not fit the `Date` partition key is an error (#5163): 20190428 days is out of the
-- range of `Date`, and wrapped modulo 65536 it would be 1984-08-15, the partition of the row with x = 3.

DROP TABLE IF EXISTS t_attach_partition_wrong_value;
CREATE TABLE t_attach_partition_wrong_value (d Date, x UInt32) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_attach_partition_wrong_value VALUES ('2019-04-28', 1), ('2019-04-28', 2), ('1984-08-15', 3);

ALTER TABLE t_attach_partition_wrong_value DETACH PARTITION 20190428; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_attach_partition_wrong_value;

ALTER TABLE t_attach_partition_wrong_value DETACH PARTITION '2019-04-28';
ALTER TABLE t_attach_partition_wrong_value DETACH PARTITION '1984-08-15';
ALTER TABLE t_attach_partition_wrong_value ATTACH PARTITION 20190428; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_attach_partition_wrong_value;

ALTER TABLE t_attach_partition_wrong_value ATTACH PARTITION '2019-04-28';
SELECT d, x FROM t_attach_partition_wrong_value ORDER BY x;

DROP TABLE t_attach_partition_wrong_value;

DROP TABLE IF EXISTS t_attach_partition_wrong_value_rmt SYNC;
CREATE TABLE t_attach_partition_wrong_value_rmt (d Date, x UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach_partition_wrong_value_rmt', 'r1') PARTITION BY d ORDER BY x;
INSERT INTO t_attach_partition_wrong_value_rmt VALUES ('2019-04-28', 1), ('2019-04-28', 2), ('1984-08-15', 3);

ALTER TABLE t_attach_partition_wrong_value_rmt DETACH PARTITION 20190428; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_attach_partition_wrong_value_rmt;

ALTER TABLE t_attach_partition_wrong_value_rmt DETACH PARTITION '2019-04-28';
ALTER TABLE t_attach_partition_wrong_value_rmt DETACH PARTITION '1984-08-15';
ALTER TABLE t_attach_partition_wrong_value_rmt ATTACH PARTITION 20190428; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_attach_partition_wrong_value_rmt;

ALTER TABLE t_attach_partition_wrong_value_rmt ATTACH PARTITION '2019-04-28';
SELECT d, x FROM t_attach_partition_wrong_value_rmt ORDER BY x;

DROP TABLE t_attach_partition_wrong_value_rmt SYNC;
