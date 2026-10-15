-- The statistics refresh task is paused while RENAME TABLE moves the parts of a table in an Ordinary database
-- and armed again afterwards, under the new name.

SET allow_deprecated_database_ordinary = 1;
-- Creation of a database with Ordinary engine emits a warning.
SET send_logs_level = 'fatal';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS refresh_statistics_interval = 300;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number FROM numbers(10);

SELECT table, count() FROM system.background_schedule_pool
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND log_name = 'MergeTreeData::refreshStatistics'
GROUP BY table ORDER BY table;

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t TO {CLICKHOUSE_DATABASE_1:Identifier}.t2;

SELECT table, count() FROM system.background_schedule_pool
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND log_name = 'MergeTreeData::refreshStatistics'
GROUP BY table ORDER BY table;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
