#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the DataLakeCatalog database engine requires the USE_AVRO build option.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stored BigLake definition combining `disk` with the Google ADC credentials (refused for a new definition)
# still loads, and survives `DETACH` plus a short `ATTACH`. None of them connects to the catalog.

workdir="${CLICKHOUSE_TMP:?}/05839_data"
rm -rf "$workdir"
mkdir -p "$workdir/metadata"
cat > "$workdir/metadata/biglake.sql" <<'SQL'
ATTACH DATABASE biglake
ENGINE = DataLakeCatalog('http://localhost:1/iceberg')
SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', disk = 'default',
    google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token'
SQL

$CLICKHOUSE_LOCAL --path "$workdir" --send_logs_level=error -m -q "
    SELECT count() FROM system.databases WHERE name = 'biglake';
    DETACH DATABASE biglake;
    ATTACH DATABASE biglake;
    SELECT count() FROM system.databases WHERE name = 'biglake';"

rm -rf "$workdir"
