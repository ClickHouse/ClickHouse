-- Tags: no-fasttest
-- no-fasttest: the `DataLakeCatalog` database engine needs the Avro and Parquet libraries.

-- A `DataLakeCatalog` database accepts the settings of the Iceberg table engine that it applies to all its tables, and
-- `ALTER DATABASE ... MODIFY SETTING` changes them for any catalog type. The other `iceberg_` settings are rejected.
-- `ATTACH` does not connect to the catalog, so the endpoint does not have to exist. `CREATE` rejects a setting before
-- it connects.

SET allow_database_iceberg = 1, allow_database_glue_catalog = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', iceberg_metadata_file_path = 'metadata/v1.metadata.json'; -- { serverError BAD_ARGUMENTS }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', iceberg_use_version_hint = 1; -- { serverError BAD_ARGUMENTS }

ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', iceberg_format_version = 1, allow_experimental_iceberg_compaction = 1,
    iceberg_compaction_delay_bias = 600;

-- The REST catalog without authentication has no settings that `ALTER DATABASE` can change, but the table settings can change.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING iceberg_compaction_delay_bias = 1200, iceberg_max_number_datafiles_to_compact = 10;
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING iceberg_metadata_file_path = 'metadata/v1.metadata.json'; -- { serverError BAD_ARGUMENTS }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING iceberg_format_version = 0; -- { serverError BAD_ARGUMENTS }
-- Nothing changes when the catalog rejects its part of the changes.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING iceberg_format_version = 2, warehouse = 'other'; -- { serverError BAD_ARGUMENTS }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A Glue catalog has no settings that `ALTER DATABASE` can change.
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181')
SETTINGS catalog_type = 'glue', warehouse = 'demo', region = 'us-east-1';

ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING allow_experimental_iceberg_compaction = 1, iceberg_timezone_for_timestamptz = 'Asia/Tokyo';
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING region = 'eu-west-1'; -- { serverError NOT_IMPLEMENTED }

SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
