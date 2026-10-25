#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real storage.
# A `BACKUP` with `structure_only` collects no data, so it must not materialize an untouched table: it neither
# loads it, nor fails when the table cannot be initialized (here: a removed named collection).

DB="${CLICKHOUSE_DATABASE}_lazy"
NC="${CLICKHOUSE_DATABASE}_nc_05452"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP NAMED COLLECTION IF EXISTS $NC;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (x String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE $DB.fuzz (x String) ENGINE = FuzzJSON($NC);
INSERT INTO $DB.mt VALUES ('a');
"

# Reloading the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; DROP NAMED COLLECTION $NC; ATTACH DATABASE $DB;"

$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.fuzz TO Memory('${CLICKHOUSE_DATABASE}_fuzz') SETTINGS structure_only = 1 FORMAT Null;
BACKUP DATABASE $DB TO Memory('${CLICKHOUSE_DATABASE}_db') SETTINGS structure_only = 1 FORMAT Null;
SELECT name, engine FROM system.tables WHERE database = '$DB' ORDER BY name;
"

# A backup with data still materializes the table and backs up its data.
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.mt TO Memory('${CLICKHOUSE_DATABASE}_mt') FORMAT Null;
SELECT name, engine FROM system.tables WHERE database = '$DB' AND name = 'mt';
DROP TABLE $DB.mt SYNC;
RESTORE TABLE $DB.mt FROM Memory('${CLICKHOUSE_DATABASE}_mt') FORMAT Null;
SELECT * FROM $DB.mt;
"

# `DROP` loads the table to check that it can be dropped.
$CLICKHOUSE_CLIENT --query "
CREATE NAMED COLLECTION $NC AS json_str = '{}';
DROP DATABASE $DB SYNC;
DROP NAMED COLLECTION $NC;
"
