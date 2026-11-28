-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: SharedMergeTree supports neither FETCH PART nor STOP REPLICATED SENDS
-- no-replicated-database: the tables use explicit Keeper paths and replica names

DROP TABLE IF EXISTS t_fetch_part_src SYNC;
DROP TABLE IF EXISTS t_fetch_part_dst SYNC;

CREATE TABLE t_fetch_part_src (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch_part_src', 'r1') ORDER BY x;
CREATE TABLE t_fetch_part_dst (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch_part_dst', 'r1') ORDER BY x;

INSERT INTO t_fetch_part_src SETTINGS insert_keeper_fault_injection_probability = 0, async_insert = 0 VALUES (1);

-- The source replica answers every part request with HTTP 500.
SYSTEM STOP REPLICATED SENDS t_fetch_part_src;

ALTER TABLE t_fetch_part_dst FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{database}/t_fetch_part_src'; -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
ALTER TABLE t_fetch_part_dst FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/t_fetch_part_src' SETTINGS max_fetch_partition_retries_count = 1; -- { serverError TOO_MANY_RETRIES_TO_FETCH_PARTS }
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fetch_part_dst';

SYSTEM START REPLICATED SENDS t_fetch_part_src;

ALTER TABLE t_fetch_part_dst FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{database}/t_fetch_part_src';
SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fetch_part_dst';

DROP TABLE t_fetch_part_src SYNC;
DROP TABLE t_fetch_part_dst SYNC;
