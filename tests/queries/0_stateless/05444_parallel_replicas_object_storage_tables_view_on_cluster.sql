-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- A view created ON CLUSTER keeps `parallel_replicas_for_object_storage_tables`, which decides how it reads its table.
DROP VIEW IF EXISTS v_05444;
CREATE VIEW v_05444 ON CLUSTER test_shard_localhost AS SELECT 1 AS x SETTINGS parallel_replicas_for_object_storage_tables = 0 FORMAT Null;
SELECT create_table_query LIKE '%parallel_replicas_for_object_storage_tables = 0%' FROM system.tables WHERE database = currentDatabase() AND name = 'v_05444';
DROP VIEW v_05444;
