-- Tags: zookeeper, no-replicated-database, need-query-parameters
-- no-replicated-database: the test creates a Replicated database of its own

-- `ALTER DATABASE ... MODIFY SETTING` of a replica-local setting of a `Replicated` database takes
-- effect on the running database, before any reattach. All the replica-local settings are published
-- together, so one consumer is observed: the initiator refuses to enqueue a query when its log
-- pointer lags behind `max_log_ptr` by more than `max_replication_lag_to_enqueue`.

SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/test/' || currentDatabase() || '/settings_runtime', 'shard1', 'replica1')
SETTINGS max_replication_lag_to_enqueue = 10;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.before_lag (x UInt8) ENGINE = Memory;

-- Make the replica lag by moving `max_log_ptr` ahead in Keeper, as if other replicas had committed
-- 100 entries this one has not executed yet. The log pointer of the replica is kept in memory, so it
-- stays where it is.
INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, toString(toUInt32(value) + 100) FROM system.zookeeper
WHERE path = '/test/' || currentDatabase() || '/settings_runtime' AND name = 'max_log_ptr';

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.after_lag (x UInt8) ENGINE = Memory; -- { serverError NOT_A_LEADER }

ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_replication_lag_to_enqueue = 1000;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.after_lag (x UInt8) ENGINE = Memory;

SELECT 'tables', name FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY name;

-- Committing the entry sets `max_log_ptr` to its number, so the replica is consistent again.
SELECT 'synced', max_log_ptr = log_ptr FROM system.database_replicas WHERE database = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
