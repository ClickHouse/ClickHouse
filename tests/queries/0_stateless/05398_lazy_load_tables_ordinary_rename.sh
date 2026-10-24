#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RENAME` in an `Ordinary` database detaches the table temporarily before it moves its data, and a detached
# lazy-load stand-in refuses to materialize the real storage. A table that had not been accessed since the
# database was loaded could not be renamed, neither within the database nor to an `Atomic` one.

DB="${CLICKHOUSE_DATABASE}_lazy_ordinary"
DB_ATOMIC="${CLICKHOUSE_DATABASE}_atomic"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --send_logs_level error --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP DATABASE IF EXISTS $DB_ATOMIC SYNC;
CREATE DATABASE $DB ENGINE = Ordinary SETTINGS lazy_load_tables = 1;
CREATE DATABASE $DB_ATOMIC ENGINE = Atomic;
CREATE TABLE $DB.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $DB.t VALUES (42);
"

# Reloading the database installs the stand-in, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --send_logs_level error --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

echo "--- within the database"
reattach
$CLICKHOUSE_CLIENT --send_logs_level error --query "RENAME TABLE $DB.t TO $DB.u"
$CLICKHOUSE_CLIENT --send_logs_level error --query "SELECT x FROM $DB.u"

echo "--- to an Atomic database"
reattach
$CLICKHOUSE_CLIENT --send_logs_level error --query "RENAME TABLE $DB.u TO $DB_ATOMIC.v"
$CLICKHOUSE_CLIENT --send_logs_level error --query "SELECT x FROM $DB_ATOMIC.v"

$CLICKHOUSE_CLIENT --send_logs_level error --query "DROP DATABASE $DB SYNC; DROP DATABASE $DB_ATOMIC SYNC;"
