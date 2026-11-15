-- A distributed INSERT SELECT with parallel replicas pins `automatic_parallel_replicas_mode` to 0, because
-- the insert should be distributed regardless of the automatic mode. The setting in the SELECT's own
-- `SETTINGS` clause must not bring the mode back: it made the initiator decline to distribute the insert
-- and run it alone. In the INSERT's `SETTINGS` clause it is shipped to the replicas, and must not stop
-- them from reading their share, otherwise every replica inserts the whole source table.
--
-- In both forms the setting must not reach the replicas in the shipped query text (the second column):
-- the parser copies the SELECT's `SETTINGS` into the INSERT's own clause, and a replica that predates
-- followers ignoring the automatic mode would re-apply it over the pinned 0 and insert every row again.
--
-- Without a local pipeline every replica of `test_cluster_one_shard_three_replicas_localhost` gets a
-- query, so a distributed insert has the initiator's own `Insert` row plus three shipped ones - 4. All
-- three replicas are this same server, so every one of those rows lands in this `system.query_log`.

SET parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;

CREATE TABLE t_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dst (k UInt64) ENGINE = Memory;

INSERT INTO t_src SELECT number FROM numbers(1e5);

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0;

SELECT '-- automatic mode in the SELECT settings';
INSERT INTO t_dst SELECT k FROM t_src SETTINGS log_comment = '05406_select_settings', automatic_parallel_replicas_mode = 2;

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(NOT is_initial_query AND position(query, 'automatic_parallel_replicas_mode') > 0) FROM system.query_log WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05406_select_settings' AND event_date >= yesterday() AND event_time >= now() - 600;

SELECT count(), uniqExact(k) FROM t_dst;

SELECT '-- automatic mode in the INSERT settings';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SETTINGS log_comment = '05406_insert_settings', automatic_parallel_replicas_mode = 2 SELECT k FROM t_src;

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(NOT is_initial_query AND position(query, 'automatic_parallel_replicas_mode') > 0) FROM system.query_log WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05406_insert_settings' AND event_date >= yesterday() AND event_time >= now() - 600;

SELECT count(), uniqExact(k) FROM t_dst;

DROP TABLE t_src;
DROP TABLE t_dst;
