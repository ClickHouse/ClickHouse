-- Tags: no-fasttest
-- no-fasttest: the `DataLakeCatalog` database engine needs the Avro and Parquet libraries.

-- `CREATE DATABASE` with `catalog_type = 'unity'` and without `use_unity_catalog_v2` stores `use_unity_catalog_v2 = 1`.
-- `CREATE` and `ATTACH` do not connect to the catalog, so the endpoint does not have to exist.

SET allow_database_unity_catalog = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181')
SETTINGS catalog_type = 'unity', warehouse = 'wh';

-- `ATTACH` reads the stored definition.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
