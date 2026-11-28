-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: the test creates its own Replicated database

-- A plain MergeTree in a Replicated database replicates a settings alter to replicas that can hold
-- different local parts, so the per-replica granularity check could let one replica commit the shared
-- metadata while another rejects the replay. index_granularity / index_granularity_bytes therefore stay
-- readonly for a table in a Replicated database, even though they are alterable on a standalone one.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier} FORMAT Null;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier}
ENGINE = Replicated('/clickhouse/05877_alter_index_granularity/{database}', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760 FORMAT Null;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t MODIFY SETTING index_granularity = 1024; -- { serverError READONLY_SETTING }
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t MODIFY SETTING index_granularity_bytes = 1000000; -- { serverError READONLY_SETTING }
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t RESET SETTING index_granularity_bytes; -- { serverError READONLY_SETTING }

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} FORMAT Null;
