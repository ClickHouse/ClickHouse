#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT` refreshes the external metadata of the table before its access check, and an `Alias` or a
# materialized view forwards the refresh to its target, materializing a target of a database with
# `lazy_load_tables`. That happened even for a user without any grants, who saw the initialization error of
# the engine (here: a removed named collection) instead of `ACCESS_DENIED`.

DB="${CLICKHOUSE_DATABASE}_lazy"
NC="${CLICKHOUSE_DATABASE}_nc_05401"
user="${CLICKHOUSE_DATABASE}_user_05401"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP USER IF EXISTS $user;
DROP NAMED COLLECTION IF EXISTS $NC;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (x String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE $DB.fuzz (x String) ENGINE = FuzzJSON($NC);
CREATE TABLE src (x String) ENGINE = Null;
CREATE TABLE alias_mt ENGINE = Alias($DB, mt);
CREATE TABLE alias_fuzz ENGINE = Alias($DB, fuzz);
CREATE MATERIALIZED VIEW mv_mt TO $DB.mt AS SELECT x FROM src;
CREATE MATERIALIZED VIEW mv_fuzz TO $DB.fuzz AS SELECT x FROM src;
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
"

# Reloading the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; DROP NAMED COLLECTION $NC; ATTACH DATABASE $DB;"

run_as()
{
    $CLICKHOUSE_CLIENT --user "$user" --password password "$@" 2>&1 \
        | grep -o -m1 -E "ACCESS_DENIED|NAMED_COLLECTION_DOESNT_EXIST" || echo "OK"
}

for table in alias_mt alias_fuzz mv_mt mv_fuzz
do
    run_as --query "INSERT INTO $table VALUES ('x')"
    run_as --async_insert 1 --wait_for_async_insert 1 --query "INSERT INTO $table VALUES ('x')"
done

$CLICKHOUSE_CLIENT --query "SELECT name, engine FROM system.tables WHERE database = '$DB' ORDER BY name"

# `DROP` loads the table to check that it can be dropped.
$CLICKHOUSE_CLIENT --query "
DROP USER $user;
DROP TABLE alias_mt;
DROP TABLE alias_fuzz;
DROP TABLE mv_mt;
DROP TABLE mv_fuzz;
CREATE NAMED COLLECTION $NC AS json_str = '{}';
DROP DATABASE $DB SYNC;
DROP NAMED COLLECTION $NC;
"
