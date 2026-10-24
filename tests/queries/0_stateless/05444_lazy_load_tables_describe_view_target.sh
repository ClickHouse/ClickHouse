#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A materialized view forwards the refresh of external metadata to its target, which `DESCRIBE` asks for.
# `DESCRIBE` of the view must not materialize a lazy-load target: it neither loads an untouched table,
# nor fails when the target cannot be initialized (here: a removed named collection).

DB="${CLICKHOUSE_DATABASE}_lazy"
NC="${CLICKHOUSE_DATABASE}_nc_05444"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP NAMED COLLECTION IF EXISTS $NC;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.src (x String) ENGINE = Null;
CREATE TABLE $DB.mt (x String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE $DB.fuzz (x String) ENGINE = FuzzJSON($NC);
CREATE MATERIALIZED VIEW $DB.mv_mt TO $DB.mt AS SELECT x FROM $DB.src;
CREATE MATERIALIZED VIEW $DB.mv_fuzz TO $DB.fuzz AS SELECT x FROM $DB.src;
"

# Reloading the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; DROP NAMED COLLECTION $NC; ATTACH DATABASE $DB;"

$CLICKHOUSE_CLIENT --query "
DESCRIBE $DB.mv_mt;
DESCRIBE $DB.mv_fuzz;
SELECT name, engine FROM system.tables WHERE database = '$DB' AND name IN ('mt', 'fuzz') ORDER BY name;
"

# Reading through the view still materializes and reads the target.
$CLICKHOUSE_CLIENT --query "
INSERT INTO $DB.mt VALUES ('a');
SELECT * FROM $DB.mv_mt;
SELECT name, engine FROM system.tables WHERE database = '$DB' AND name = 'mt';
"

# `DROP` loads the table to check that it can be dropped.
$CLICKHOUSE_CLIENT --query "
CREATE NAMED COLLECTION $NC AS json_str = '{}';
DROP DATABASE $DB SYNC;
DROP NAMED COLLECTION $NC;
"
