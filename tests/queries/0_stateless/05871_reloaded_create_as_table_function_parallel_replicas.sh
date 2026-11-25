#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# A table created `AS icebergLocal(...)` is read after `DETACH DATABASE` / `ATTACH DATABASE` the way it is read after `CREATE`.
# The tables are loaded with the server profile, which enables parallel replicas in the `ParallelReplicas` job.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_reload"
DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/"
rm -rf "$DIR"
trap 'rm -rf "$DIR"' EXIT

$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE TABLE $DB.t (id Int64) ENGINE = IcebergLocal('$DIR', 'Parquet');
    INSERT INTO $DB.t SETTINGS allow_insert_into_iceberg = 1, async_insert = 0 VALUES (100);
    CREATE TABLE $DB.tf (id Int64) AS icebergLocal('$DIR', 'Parquet');
    DETACH DATABASE $DB;
    ATTACH DATABASE $DB;
    SELECT sum(id) FROM $DB.tf;
    SELECT * FROM ((SELECT 'a', sum(id) FROM $DB.tf) UNION ALL (SELECT 'b', sum(id) FROM $DB.tf)) ORDER BY 1;
    DROP DATABASE $DB SYNC;
"
