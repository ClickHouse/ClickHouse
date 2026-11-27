-- Tags: zookeeper

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Replicated('/test/{database}_1', 's1', 'r1');

SET distributed_ddl_output_mode = 'none';
SET enable_elasticsearch = 1;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.es_function AS elasticsearch('http://localhost:1', 'logs');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.es_engine (document JSON) ENGINE = Elasticsearch('http://localhost:1', 'logs');

-- A new replica restores the tables from the stored definitions, without the settings of the session that created them.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/{database}', 's1', 'r2');
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
SYSTEM SYNC DATABASE REPLICA {CLICKHOUSE_DATABASE_1:Identifier};
SELECT name, engine FROM system.tables WHERE database = currentDatabase() || '_1' ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
