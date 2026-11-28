-- Tags: no-replicated-database
-- ^ due to the usage of ON CLUSTER queries

-- ON CLUSTER replaces `currentDatabase()` with a literal before queueing the query, keeping its alias in both forms.
SET distributed_ddl_output_mode = 'none';

DROP VIEW IF EXISTS v_alias_cluster;
CREATE VIEW v_alias_cluster ON CLUSTER test_shard_localhost AS SELECT currentDatabase() AS {a:Identifier}, currentDatabase() AS b;
SELECT parameterized_view_parameters, position(create_table_query, '{a:Identifier}') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_alias_cluster';
SELECT x = currentDatabase(), b = currentDatabase() FROM v_alias_cluster(a = 'x');
DROP VIEW v_alias_cluster;
