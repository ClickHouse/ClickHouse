#!/usr/bin/env bash

# A `Merge` table reads a subcolumn that exists only in its own type of a column (here `s.x` of a `String` child
# column `s` exposed as `JSON`) through the whole child column, so the access check of the child authorizes that
# column: a grant on `s` allows `s.x`, and a revoke on `s` denies it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05569_${CLICKHOUSE_DATABASE}"
child="${CLICKHOUSE_DATABASE}.child"
m="${CLICKHOUSE_DATABASE}.m"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE child (s String, other UInt8) ENGINE = Memory;
INSERT INTO child VALUES ('{\"x\":789}', 1);
CREATE TABLE m (s JSON, other UInt8) ENGINE = Merge(currentDatabase(), '^child$');
CREATE USER $user;
"

run()
{
    local out
    for query in "SELECT s.x FROM m" "SELECT s FROM m"; do
        if out=$($CLICKHOUSE_CLIENT --user "$user" -q "$query" 2>&1); then
            echo "$1: $query -> $out"
        elif echo "$out" | grep -q 'ACCESS_DENIED'; then
            echo "$1: $query -> ACCESS_DENIED"
        else
            echo "$1: $query -> $out"
        fi
    done
}

reset()
{
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user; GRANT SELECT ON $m TO $user"
}

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(s) ON $child TO $user"
run "GRANT(s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $child TO $user; REVOKE SELECT(s) ON $child FROM $user"
run "GRANT ON child REVOKE(s)"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
