#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A directory at the `<table>.sql.detached` path is not a flag, but unlink() of it fails (EISDIR):
# CREATE TABLE must not fail on it. A real leftover flag must still be removed.

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

${CLICKHOUSE_LOCAL} --path="$DIR" --query "CREATE DATABASE db ENGINE = Atomic"
META=$(${CLICKHOUSE_LOCAL} --path="$DIR" --query "SELECT metadata_path FROM system.databases WHERE name = 'db'")
[[ "$META" = /* ]] || META="$DIR/$META"

mkdir -p "$META"
mkdir "$META/t1.sql.detached"
touch "$META/t2.sql.detached"

${CLICKHOUSE_LOCAL} --path="$DIR" --query "
    CREATE TABLE db.t1 (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE db.t2 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db.t1 VALUES (1);
    INSERT INTO db.t2 VALUES (2);"
[ -e "$META/t2.sql.detached" ] && echo "t2 flag was not removed"

# After a restart both tables are attached.
${CLICKHOUSE_LOCAL} --path="$DIR" --query "SELECT id FROM db.t1; SELECT id FROM db.t2"

rm -rf "$DIR"
