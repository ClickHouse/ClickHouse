#!/usr/bin/env bash
# Renaming a column whose subcolumn is used in a TTL that an older server stored with an alias is not refused
# (05874_rename_column_subcolumn_references covers the definitions without aliases).
#
# Such a definition can no longer be written through SQL, so the stored metadata of a table
# created without the alias is edited into the form the older server wrote.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (s Tuple(x DateTime), v UInt32 TTL s.x + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY tuple() TTL s.x + INTERVAL 1 DAY;
INSERT INTO db.t VALUES (tuple(toDateTime('2100-01-01 00:00:00')), 3);
"

metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/TTL s.x + /TTL (s.x AS e) + /g' "${metadata_file}"
# Without this the rename below would run against an unmodified definition, i.e. assert nothing.
grep -c -F 'TTL (s.x AS e) + ' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ALTER TABLE db.t RENAME COLUMN s TO renamed_s;
SELECT countSubstrings(create_table_query, 'TTL (renamed_s.x AS e) + toIntervalDay(1)') FROM system.tables WHERE database = 'db' AND name = 't';
SELECT renamed_s.x, v FROM db.t;
"

rm -rf "${WORKING_DIR}"
