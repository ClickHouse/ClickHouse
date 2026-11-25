#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A backed-up BigLake definition combining `disk` with Google credentials (refused by `CREATE`) is restored
# without `disk`, as `CREATE` accepts it. Neither the backup nor the restore connects to the catalog.

workdir="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$workdir"
mkdir -p "$workdir/metadata" "$workdir/backups"
cat > "$workdir/metadata/biglake.sql" <<'SQL'
ATTACH DATABASE biglake
ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default',
    google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token'
SQL

$CLICKHOUSE_LOCAL --path "$workdir" --send_logs_level=error -m -q "
    SET allow_database_iceberg = 1;
    BACKUP DATABASE biglake TO File('$workdir/backups/biglake') FORMAT Null;
    RESTORE DATABASE biglake AS restored FROM File('$workdir/backups/biglake') FORMAT Null;
    SELECT name, position(engine_full, 'disk') > 0 FROM system.databases WHERE name IN ('biglake', 'restored') ORDER BY name;" \
    -- --backups.allowed_path="$workdir/backups"

rm -rf "$workdir"
