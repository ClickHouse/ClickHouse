-- Statistics are not declared per column anymore, they are collected according to the `auto_statistics_types` setting.
-- Entries and metadata stored by older versions can still refer to explicitly declared statistics.

DROP TABLE IF EXISTS t_replay_materialize_statistics;

CREATE TABLE t_replay_materialize_statistics (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';

INSERT INTO t_replay_materialize_statistics SELECT number FROM numbers(100);

-- A new submission is validated: the column has no statistics.
ALTER TABLE t_replay_materialize_statistics MATERIALIZE STATISTICS a; -- { serverError ILLEGAL_STATISTICS }

-- An entry of the distributed DDL queue could have been accepted by an older version for a column with explicitly
-- declared statistics. It is executed by the DDL worker, and it must not fail there.
ALTER TABLE t_replay_materialize_statistics ON CLUSTER test_shard_localhost MATERIALIZE STATISTICS a
SETTINGS distributed_ddl_output_mode = 'none', mutations_sync = 2;

SELECT count(), countIf(is_done), countIf(latest_fail_reason != '') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_replay_materialize_statistics';

SELECT count(), sum(a) FROM t_replay_materialize_statistics;

DROP TABLE t_replay_materialize_statistics;

-- A structure string stored by an older version in the definition of a table created from a table function
-- can contain a `STATISTICS` clause. It is ignored, so that the table is still attached.
DROP TABLE IF EXISTS t_structure_with_statistics;

CREATE TABLE t_structure_with_statistics AS file('data_05621_nonexistent.csv', 'CSV', 'a UInt64 STATISTICS(tdigest), b String');
DETACH TABLE t_structure_with_statistics;
ATTACH TABLE t_structure_with_statistics;
DESCRIBE TABLE t_structure_with_statistics;

DROP TABLE t_structure_with_statistics;
