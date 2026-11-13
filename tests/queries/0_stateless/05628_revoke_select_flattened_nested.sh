#!/usr/bin/env bash

# A column `nt.x` of a flattened `Nested` structure `nt` is denied by `REVOKE SELECT(nt)` unless it has a grant or
# revoke of its own, on every path that checks `SELECT` on named columns. A grant on `nt` alone grants nothing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05628_${CLICKHOUSE_DATABASE}"
role="r_05628_${CLICKHOUSE_DATABASE}"
definer="d_05628_${CLICKHOUSE_DATABASE}"
db="$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user, $definer; DROP ROLE IF EXISTS $role; CREATE USER $user; CREATE USER $definer; CREATE ROLE $role;"
$CLICKHOUSE_CLIENT --flatten_nested 1 -m -q "
CREATE TABLE t (id UInt8, nt Nested(public String, secret String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tn (nt Nested(public String, secret String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE s (nt Nested(a String)) ENGINE = Set;
CREATE TABLE td (id UInt8, \`n.name\` Nested(secret String)) ENGINE = MergeTree ORDER BY tuple();
"
$CLICKHOUSE_CLIENT --flatten_nested 0 -q "CREATE TABLE t0 (id UInt8, nt Nested(public String, secret String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -m -q "
INSERT INTO t VALUES (1, ['p'], ['s']);
INSERT INTO tn VALUES (['p'], ['s']);
INSERT INTO s VALUES (['p']);
INSERT INTO t0 VALUES (1, [('p', 's')]);
CREATE TABLE m ENGINE = Merge(currentDatabase(), '^tn\$');
SET allow_experimental_alias_table_engine = 1;
CREATE TABLE al ENGINE = Alias('$db', 'tn');
CREATE VIEW v SQL SECURITY DEFINER AS SELECT * FROM tn;
"
$CLICKHOUSE_CLIENT -q "SELECT table, arraySort(groupArray(name)) FROM system.columns WHERE database = currentDatabase() AND table IN ('t', 't0', 'tn', 's') GROUP BY table ORDER BY table"

# Prints `+` or `-` for each query run by the user.
check()
{
    local line="$1:" out
    shift
    for query in "$@"; do
        if out=$($CLICKHOUSE_CLIENT --user "$user" -q "$query FORMAT Null" 2>&1); then
            line="$line +"
        elif echo "$out" | grep -q 'ACCESS_DENIED'; then
            line="$line -"
        else
            line="$line $out"
        fi
    done
    echo "$line"
}

reads=(
    "SELECT id FROM t"
    "SELECT nt FROM t"
    "SELECT nt.public FROM t"
    "SELECT nt.secret FROM t"
    "SELECT length(nt.secret) FROM t SETTINGS optimize_functions_to_subcolumns = 1"
    "SELECT nt.secret FROM t ARRAY JOIN nt"
    "SELECT count() FROM t"
)
reads0=("SELECT nt FROM t0" "SELECT nt.public FROM t0" "SELECT nt.secret FROM t0")
carriers=(
    "SELECT count() FROM tn SETTINGS optimize_trivial_count_query = 1"
    "SELECT count() FROM m SETTINGS optimize_trivial_count_query = 1, apply_patch_parts = 0"
    "SELECT count() FROM al SETTINGS optimize_trivial_count_query = 1"
    "SELECT ['p'] IN s"
    "SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), tn)"
    "SELECT nt.secret FROM v SETTINGS analyzer_inline_views = 1"
)

reset() { $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user; REVOKE ALL ON *.* FROM $role; REVOKE $role FROM $user"; }

reset; check "no grants" "${reads[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(nt) ON $db.t TO $user"; check "GRANT(nt)" "${reads[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`nt.public\`) ON $db.t TO $user"; check "GRANT(nt.public)" "${reads[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.t TO $user; REVOKE SELECT(nt) ON $db.t FROM $user"; check "GRANT table, REVOKE(nt)" "${reads[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.t TO $user; REVOKE SELECT(\`nt.secret\`) ON $db.t FROM $user"; check "GRANT table, REVOKE(nt.secret)" "${reads[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.t TO $role; REVOKE SELECT(nt) ON $db.t FROM $role; GRANT $role TO $user"; check "role: GRANT table, REVOKE(nt)" "${reads[@]}"
# With flatten_nested = 0 the same declaration is one column with subcolumns, and the revoke behaves the same.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.t0 TO $user; REVOKE SELECT(nt) ON $db.t0 FROM $user"; check "t0 GRANT table, REVOKE(nt)" "${reads0[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.tn TO $user; GRANT SELECT ON $db.m TO $user; GRANT SELECT ON $db.al TO $user; GRANT SELECT ON $db.s TO $user; GRANT SELECT ON $db.v TO $user"
check "carriers, no revoke" "${carriers[@]}"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(nt) ON $db.tn FROM $user; REVOKE SELECT(nt) ON $db.s FROM $user; REVOKE SELECT(nt) ON $db.v FROM $user"
check "carriers, REVOKE(nt)" "${carriers[@]}"

# The name of a `Nested` column may contain a dot.
reads_td=("SELECT id FROM td" "SELECT n.name.secret FROM td" "SELECT n FROM td")
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.td TO $user; REVOKE SELECT(\`n.name\`) ON $db.td FROM $user"; check "td GRANT table, REVOKE(n.name)" "${reads_td[@]}"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.td TO $user; REVOKE SELECT(n) ON $db.td FROM $user"; check "td GRANT table, REVOKE(n)" "${reads_td[@]}"

# A materialized view checks its definer's grants on the source-table columns named like the view's. The view may store
# an element of the source's `Nested` structure with another type, and may outlive its source table.
$CLICKHOUSE_CLIENT --flatten_nested 1 -m -q "
CREATE TABLE ts (id UInt8, nt Nested(secret String), \`x.y\` String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tgt (id UInt8, \`nt.secret\` String, \`x.y\` String) ENGINE = MergeTree ORDER BY tuple();
GRANT SELECT ON $db.ts TO $definer;
GRANT SELECT ON $db.tgt TO $definer;
CREATE MATERIALIZED VIEW mv TO tgt DEFINER = $definer SQL SECURITY DEFINER AS SELECT id, arrayJoin(nt.secret) AS \`nt.secret\`, \`x.y\` FROM ts;
"
reads_mv=("SELECT id FROM mv" "SELECT nt.secret FROM mv" "SELECT x.y FROM mv")
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.mv TO $user"; check "mv, no revoke" "${reads_mv[@]}"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(nt) ON $db.ts FROM $definer; REVOKE SELECT(x) ON $db.ts FROM $definer"
check "mv, definer REVOKE(nt), REVOKE(x)" "${reads_mv[@]}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts"; check "mv, definer REVOKE(nt), REVOKE(x), source dropped" "${reads_mv[@]}"

# A view column no revoke can apply to is read without resolving the source table, which may be unavailable. A source
# that has to be resolved but fails to counts as missing, so neither its errors nor its data reach the reader of the view.
mvdb="${db}_mv"
urldb="${db}_url"
src_file="${CLICKHOUSE_USER_FILES_UNIQUE}/source.jsonl"
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
echo '{"id":1,"nt.secret":["s"]}' > "$src_file"
# An old mtime, so that the rewritten file does not match the schema cached when the view is created.
touch -d @1000000000 "$src_file"
$CLICKHOUSE_CLIENT -m -q "
CREATE DATABASE $urldb ENGINE = URL('file://');
CREATE DATABASE $mvdb ENGINE = Atomic;
GRANT SELECT ON $urldb.* TO $definer;
GRANT READ ON FILE TO $definer;
CREATE MATERIALIZED VIEW $mvdb.mv ENGINE = MergeTree ORDER BY tuple() DEFINER = $definer SQL SECURITY DEFINER
    AS SELECT id, \`nt.secret\` FROM $urldb.\`$src_file\`;
INSERT INTO $mvdb.mv VALUES (42, ['stored']);
GRANT SELECT ON $mvdb.mv TO $user;
REVOKE SELECT(nt) ON $urldb.\`$src_file\` FROM $definer;
"
reads_url=("SELECT id FROM $mvdb.mv" "SELECT nt.secret FROM $mvdb.mv")
check "mv over a URL source, definer REVOKE(nt)" "${reads_url[@]}"
echo '{"id":1,"nt.secret":["s"],"x":LEAKED_SOURCE_BYTES}' > "$src_file"
check "mv over a URL source, definer REVOKE(nt), source unreadable" "${reads_url[@]}"
$CLICKHOUSE_CLIENT --user "$user" --send_logs_level debug -q "SELECT nt.secret FROM $mvdb.mv" 2>&1 | grep -c LEAKED_SOURCE_BYTES
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON $urldb.* FROM $definer"
check "mv over a URL source, definer REVOKE source, source unreadable" "${reads_url[@]}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $mvdb; DROP DATABASE $urldb;"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
$CLICKHOUSE_CLIENT -q "DROP VIEW mv; DROP TABLE tgt; DROP USER $user, $definer; DROP ROLE $role; DROP VIEW v; DROP TABLE al; DROP TABLE m; DROP TABLE s; DROP TABLE td; DROP TABLE tn; DROP TABLE t; DROP TABLE t0;"
