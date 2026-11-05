#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_1"

# CREATE leaves a name that resolves to a temporary table of the session unqualified,
# also when the database has a table of that name.
$CLICKHOUSE_CLIENT --query "
CREATE DATABASE ${DB} ENGINE = Atomic;
USE ${DB};
CREATE TABLE ${DB}.tmp_shadow (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB}.tmp_shadow VALUES (2);
CREATE TEMPORARY TABLE tmp_src (id UInt64);
CREATE TEMPORARY TABLE tmp_shadow (id UInt64);
CREATE VIEW ${DB}.v_from AS SELECT id FROM tmp_src;
CREATE VIEW ${DB}.v_join AS SELECT n.number AS id FROM numbers(3) AS n INNER JOIN tmp_src AS t ON n.number = t.id;
CREATE VIEW ${DB}.v_in AS SELECT number AS id FROM numbers(3) WHERE number IN tmp_src;
CREATE VIEW ${DB}.v_scalar AS SELECT (SELECT max(id) FROM tmp_src) AS id;
CREATE VIEW ${DB}.v_shadow AS SELECT id FROM tmp_shadow;
"

# Reload the views from their metadata in a session without the temporary table.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE ${DB}"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE ${DB}"

# A new session reads its own temporary table through the views.
for view in v_from v_join v_in v_scalar v_shadow; do
    $CLICKHOUSE_CLIENT --query "
    CREATE TEMPORARY TABLE tmp_src (id UInt64);
    CREATE TEMPORARY TABLE tmp_shadow (id UInt64);
    INSERT INTO tmp_src VALUES (1);
    INSERT INTO tmp_shadow VALUES (1);
    SELECT '${view}', groupArray(id) FROM ${DB}.${view};
    " 2>&1 | grep -m1 -oE "^v_[a-z]+\s\[[0-9,]*\]$|UNKNOWN_[A-Z]+"
done

$CLICKHOUSE_CLIENT --query "DROP DATABASE ${DB}"
