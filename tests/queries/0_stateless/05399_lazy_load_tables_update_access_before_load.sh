#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `UPDATE` classifies `_row_exists` against the real storage before the access check, which materialized
# a table of a database with `lazy_load_tables` even for a user without any grants, and reported the
# initialization error of the engine (here: a removed named collection) to them instead of `ACCESS_DENIED`.

DB="${CLICKHOUSE_DATABASE}_lazy"
NC="${CLICKHOUSE_DATABASE}_nc_05399"
user="${CLICKHOUSE_DATABASE}_user_05399"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP USER IF EXISTS $user;
DROP NAMED COLLECTION IF EXISTS $NC;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO $DB.mt VALUES (1);
CREATE TABLE $DB.fuzz (x String) ENGINE = FuzzJSON($NC);
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
"

# Reloading the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; DROP NAMED COLLECTION $NC; ATTACH DATABASE $DB;"

run_as()
{
    $CLICKHOUSE_CLIENT --user "$user" --password password --enable_lightweight_update 1 --query "$*" 2>&1 \
        | grep -o -m1 -E "ACCESS_DENIED|NAMED_COLLECTION_DOESNT_EXIST" || echo "OK"
}

run_as "UPDATE $DB.mt SET x = 10 WHERE 1"
run_as "UPDATE $DB.mt SET _row_exists = 0 WHERE 1"
run_as "UPDATE $DB.fuzz SET x = 'changed' WHERE 1"

$CLICKHOUSE_CLIENT --query "SELECT name, engine FROM system.tables WHERE database = '$DB' ORDER BY name"

# `DROP` loads the table to check that it can be dropped.
$CLICKHOUSE_CLIENT --query "
DROP USER $user;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
DROP DATABASE $DB SYNC;
DROP NAMED COLLECTION $NC;
"
