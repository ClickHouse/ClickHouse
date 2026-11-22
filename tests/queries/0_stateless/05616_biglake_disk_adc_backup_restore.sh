#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

# A BigLake database whose stored definition combines `disk` with the Google ADC credentials (refused on a new CREATE)
# is restored from a backup. None of the statements connects to the catalog.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_biglake"
db_restored="${CLICKHOUSE_DATABASE}_biglake_restored"
backup="Disk('backups', '${CLICKHOUSE_DATABASE}_05616')"

# Such a definition is accepted with a warning in the server log, which must not reach stderr.
$CLICKHOUSE_CLIENT --send_logs_level=error -m -q "
    SET allow_database_iceberg = 1;
    ATTACH DATABASE ${db} ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
    SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token';
    BACKUP DATABASE ${db} TO ${backup} FORMAT Null;
    RESTORE DATABASE ${db} AS ${db_restored} FROM ${backup} FORMAT Null;
    SELECT count() FROM system.databases WHERE name = '${db_restored}';
    DROP DATABASE ${db};
    DROP DATABASE ${db_restored};
"
