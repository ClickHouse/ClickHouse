-- The `Dictionary` database engine used to put its comment in back quotes, so `SHOW CREATE DATABASE` and any query
-- that reads `system.databases.engine_full` failed with `SYNTAX_ERROR`.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Dictionary COMMENT 'Test DB with \'comment\'';

SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT TSVRaw;
SELECT engine_full, comment FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String} FORMAT TSVRaw;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
