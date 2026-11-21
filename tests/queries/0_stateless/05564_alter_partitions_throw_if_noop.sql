-- Tags: zookeeper
-- zookeeper: the setting is also checked for ReplicatedMergeTree.

DROP TABLE IF EXISTS t_noop;
DROP TABLE IF EXISTS t_noop_replicated;

CREATE TABLE t_noop (d Date, x UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
CREATE TABLE t_noop_replicated (d Date, x UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_noop_replicated', 'r1') PARTITION BY toYYYYMM(d) ORDER BY x;

INSERT INTO t_noop VALUES ('2019-01-01', 1), ('2019-02-01', 2);
INSERT INTO t_noop_replicated VALUES ('2019-01-01', 1), ('2019-02-01', 2);

-- By default, a partition that matches no parts is a no-op.
ALTER TABLE t_noop DROP PARTITION 201903;
ALTER TABLE t_noop_replicated DETACH PARTITION 201903;

SET alter_partitions_throw_if_noop = 1;

SELECT 'MergeTree';
-- `YYYYMMDD` instead of `YYYYMM`: the partition ID is valid but matches nothing.
ALTER TABLE t_noop DROP PARTITION 20190101; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop DROP PARTITION 201903; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop DETACH PARTITION ID '201903'; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop DETACH PARTITION 201901;
ALTER TABLE t_noop DETACH PARTITION 201901; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop ATTACH PARTITION 201901;
ALTER TABLE t_noop DROP PARTITION ID '201902';
SELECT d, x FROM t_noop ORDER BY x;
ALTER TABLE t_noop DROP PARTITION ALL;
ALTER TABLE t_noop DETACH PARTITION ALL;
SELECT count() FROM t_noop;

SELECT 'ReplicatedMergeTree';
ALTER TABLE t_noop_replicated DROP PARTITION 201903; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop_replicated DETACH PARTITION ID '201903'; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop_replicated DETACH PARTITION 201901;
ALTER TABLE t_noop_replicated DETACH PARTITION 201901; -- { serverError PARTITION_DOESNT_EXIST }
ALTER TABLE t_noop_replicated ATTACH PARTITION 201901;
ALTER TABLE t_noop_replicated DROP PARTITION ID '201902';
SELECT d, x FROM t_noop_replicated ORDER BY x;
ALTER TABLE t_noop_replicated DROP PARTITION ALL;
ALTER TABLE t_noop_replicated DETACH PARTITION ALL;
SELECT count() FROM t_noop_replicated;

DROP TABLE t_noop;
DROP TABLE t_noop_replicated;
