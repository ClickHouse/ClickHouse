#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

# A `DataLakeCatalog` database whose stored definition has a table setting that a new `CREATE DATABASE` rejects, such as
# `paimon_metadata_refresh_interval_sec`, which older versions accepted, is restored from a backup. None of the statements
# connects to the catalog.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_lake"
db_restored="${CLICKHOUSE_DATABASE}_lake_restored"
backup="Disk('backups', '${CLICKHOUSE_DATABASE}_05876')"

$CLICKHOUSE_CLIENT -m -q "
    SET allow_database_iceberg = 1;
    CREATE DATABASE ${db} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
    SETTINGS catalog_type = 'rest', warehouse = 'demo', paimon_metadata_refresh_interval_sec = 0; -- { serverError BAD_ARGUMENTS }
    ATTACH DATABASE ${db} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
    SETTINGS catalog_type = 'rest', warehouse = 'demo', paimon_metadata_refresh_interval_sec = 0;
    BACKUP DATABASE ${db} TO ${backup} FORMAT Null;
    RESTORE DATABASE ${db} AS ${db_restored} FROM ${backup} FORMAT Null;
    SELECT engine_full LIKE '%paimon_metadata_refresh_interval_sec = 0%' FROM system.databases WHERE name = '${db_restored}';
    DROP DATABASE ${db};
    DROP DATABASE ${db_restored};
"
