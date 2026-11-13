-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed in a Replicated database

-- An explicit declaration of statistics is rejected on the initiator of `ON CLUSTER`, before the query is routed,
-- because the DDL workers ignore it to be able to replay the entries accepted by older versions.

DROP TABLE IF EXISTS t_stat_on_cluster SYNC;

SET distributed_ddl_output_mode = 'none';

CREATE TABLE t_stat_on_cluster ON CLUSTER test_shard_localhost (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
SET distributed_ddl_entry_format_version = 1;
CREATE TABLE t_stat_on_cluster ON CLUSTER test_shard_localhost (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
SET distributed_ddl_entry_format_version = DEFAULT;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_stat_on_cluster';

CREATE TABLE t_stat_on_cluster ON CLUSTER test_shard_localhost (a UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_stat_on_cluster';

DROP TABLE t_stat_on_cluster SYNC;
