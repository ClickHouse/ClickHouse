-- Tags: no-fasttest

DROP TABLE IF EXISTS runner;
DROP TABLE IF EXISTS runner_async;
DROP TABLE IF EXISTS runner_cluster;
CREATE TABLE runner (query String, database String, settings Map(String, String)) ENGINE = QueryRunner SETTINGS mode = 'synchronous';
CREATE TABLE runner_async (query String, database String) ENGINE = QueryRunner;
CREATE TABLE runner_cluster (query String, database String, settings Map(String, String)) ENGINE = QueryRunner
    SETTINGS cluster = 'test_cluster_interserver_secret', mode = 'synchronous';
-- A token generated per run keeps the tables and the jobs of an earlier run in the same database out of the lookups below.
CREATE TEMPORARY TABLE run AS SELECT toString(rand64()) AS id;

INSERT INTO runner (query, database) SELECT 'CREATE TABLE ' || currentDatabase() || '.t_allowed_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase();
INSERT INTO runner (query, database) SETTINGS allow_ddl = 0 SELECT 'CREATE TABLE ' || currentDatabase() || '.t_sync_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase();
INSERT INTO runner_async (query, database) SETTINGS allow_ddl = 0 SELECT 'CREATE TABLE ' || currentDatabase() || '.t_async_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase();
SYSTEM WAIT QUERY RUNNER runner_async;
INSERT INTO runner (query, database, settings) SETTINGS allow_ddl = 0 SELECT 'CREATE TABLE ' || currentDatabase() || '.t_lift_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase(), map('allow_ddl', '1');
INSERT INTO runner_cluster (query, database) SETTINGS allow_ddl = 0 SELECT 'CREATE TABLE ' || currentDatabase() || '.t_cluster_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase();
INSERT INTO runner_cluster (query, database, settings) SETTINGS allow_ddl = 0 SELECT 'CREATE TABLE ' || currentDatabase() || '.t_cluster_lift_' || (SELECT id FROM run) || ' (x UInt8) ENGINE = Memory', currentDatabase(), map('allow_ddl', '1');

SYSTEM FLUSH LOGS query_log;
-- How each job ended, from the QueryRunner side (`t_lift` is refused before its query starts, so it has no row).
SELECT replaceOne(extract(query, '\\.(t_[a-z_]+_[0-9]+) '), '_' || (SELECT id FROM run), '') AS t, type, errorCodeToName(exception_code)
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_internal AND client_name = 'ClickHouse QueryRunner'
    AND query LIKE 'CREATE TABLE ' || currentDatabase() || '.t\_%\_' || (SELECT id FROM run) || ' %' AND type != 'QueryStart'
ORDER BY t;
SELECT replaceOne(name, '_' || (SELECT id FROM run), '') AS t FROM system.tables
WHERE database = currentDatabase() AND name LIKE 't\_%\_' || (SELECT id FROM run) ORDER BY t;
INSERT INTO runner (query, database) SELECT 'DROP TABLE ' || currentDatabase() || '.t_allowed_' || (SELECT id FROM run), currentDatabase();
DROP TABLE runner;
DROP TABLE runner_async;
DROP TABLE runner_cluster;
