-- Tags: no-fasttest
-- no-fasttest: the `DataLakeCatalog` database engine needs the Avro and Parquet libraries.

-- `snapshot_loading_mode` accepts only an empty value, 'all' and 'refs'.
-- `ATTACH` does not connect to the catalog, so the endpoint does not have to exist.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', snapshot_loading_mode = 'bogus'; -- { serverError BAD_ARGUMENTS }

ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', snapshot_loading_mode = 'REFS'; -- { serverError BAD_ARGUMENTS }

ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', snapshot_loading_mode = 'refs';

SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
