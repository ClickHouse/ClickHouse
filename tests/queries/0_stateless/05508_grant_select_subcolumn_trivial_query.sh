#!/usr/bin/env bash

# A grant on the exact name of a real subcolumn (e.g. ``GRANT SELECT(`j.a`)``) is enough for a trivial query
# such as `SELECT count()`, the same way a grant on any one column is, while a grant on a name that is not a
# subcolumn of the table is not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05508_${CLICKHOUSE_DATABASE}"
t="${CLICKHOUSE_DATABASE}.t"
a="${CLICKHOUSE_DATABASE}.a"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t (secret String, p Tuple(s String, o String), j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES ('x', ('s', 'o'), '{\"a\": \"a\"}'), ('y', ('s', 'o'), '{\"a\": \"b\"}');
CREATE TABLE a ENGINE = Alias(currentDatabase(), t);
CREATE USER $user;
"

check()
{
    local out
    for query in "SELECT count() FROM $1" "SELECT 1 FROM $1 LIMIT 1"; do
        for analyzer in 1 0; do
            if out=$($CLICKHOUSE_CLIENT --user "$user" --enable_analyzer $analyzer -q "$query" 2>&1); then
                echo "$2: analyzer=$analyzer $query -> $out" | sed "s/${CLICKHOUSE_DATABASE}\.//"
            elif echo "$out" | grep -q 'ACCESS_DENIED'; then
                echo "$2: analyzer=$analyzer $query -> ACCESS_DENIED" | sed "s/${CLICKHOUSE_DATABASE}\.//"
            else
                echo "$2: analyzer=$analyzer $query -> $out"
            fi
        done
    done
}

reset()
{
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user"
}

reset
check "$t" "no grants"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`j.a\`) ON $t TO $user"
check "$t" "GRANT(j.a)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$t" "GRANT(p.s)"

# Not a subcolumn: `secret` is a `String`.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`secret.x\`) ON $t TO $user"
check "$t" "GRANT(secret.x)"

# An `Alias` also requires the grant on its target table.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`j.a\`) ON $a TO $user"
check "$a" "alias GRANT(j.a)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`j.a\`) ON $a TO $user; GRANT SELECT(\`j.a\`) ON $t TO $user"
check "$a" "alias and target GRANT(j.a)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`j.a\`) ON $a TO $user; GRANT SELECT(j) ON $t TO $user"
check "$a" "alias GRANT(j.a), target GRANT(j)"

# The exact subcolumn grant can also be on the target, while the alias grants the whole table or the column.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $a TO $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a" "alias GRANT ON a, target GRANT(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $a TO $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a" "alias GRANT(p), target GRANT(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(j) ON $a TO $user; GRANT SELECT(\`j.a\`) ON $t TO $user"
check "$a" "alias GRANT(j), target GRANT(j.a)"

# The alias must still grant the subcolumn, through its column or by its exact name.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $a TO $user; REVOKE SELECT(p) ON $a FROM $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a" "alias GRANT ON a REVOKE(p), target GRANT(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(secret) ON $a TO $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a" "alias GRANT(secret), target GRANT(p.s)"

# Each hop of a chain of aliases decides the subcolumn on its own: here `a1` grants the table, `a2` grants the column
# and `t` grants the exact subcolumn, so `p.s` is readable, as it is for `SELECT p.s FROM a1`. An alias of an existing
# alias cannot be created, so `a1` is created before its target `a2`.
a1="${CLICKHOUSE_DATABASE}.a1"
a2="${CLICKHOUSE_DATABASE}.a2"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE a1 ENGINE = Alias(currentDatabase(), a2);
CREATE TABLE a2 ENGINE = Alias(currentDatabase(), t);
"
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $a1 TO $user; GRANT SELECT(p) ON $a2 TO $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a1" "chain GRANT ON a1, GRANT(p) ON a2, GRANT(p.s) ON t"

# The middle hop must grant the subcolumn too, here it grants only another column.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $a1 TO $user; GRANT SELECT(secret) ON $a2 TO $user; GRANT SELECT(\`p.s\`) ON $t TO $user"
check "$a1" "chain GRANT ON a1, GRANT(secret) ON a2, GRANT(p.s) ON t"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
