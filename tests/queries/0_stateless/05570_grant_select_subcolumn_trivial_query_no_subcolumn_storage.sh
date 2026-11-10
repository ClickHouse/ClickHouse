#!/usr/bin/env bash

# A trivial query (`SELECT 1`, `SELECT count()`) over a storage that does not support subcolumns
# (`StripeLog`, `Join`) must read a whole column even when the user has a grant on the exact name of a
# subcolumn: such storages cannot read a subcolumn, so adding that grant must not break the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05570_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE s (p Tuple(s UInt8), other String) ENGINE = StripeLog;
INSERT INTO s VALUES ((7), 'abc'), ((8), 'def');
CREATE TABLE j (k UInt8, p Tuple(s UInt8), other String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j VALUES (1, (7), 'abc'), (2, (8), 'def');
CREATE TABLE a ENGINE = Alias(currentDatabase(), s);
CREATE USER $user;
"

check()
{
    local out
    for query in "SELECT 1 FROM $CLICKHOUSE_DATABASE.$1 LIMIT 1" "SELECT count() FROM $CLICKHOUSE_DATABASE.$1"; do
        if out=$($CLICKHOUSE_CLIENT --user "$user" -q "$query" 2>&1); then
            echo "$2: $query -> $out" | sed "s/${CLICKHOUSE_DATABASE}\.//"
        else
            echo "$2: $query -> $(echo "$out" | grep -o -m1 'Code: [0-9]*. DB::Exception[^(]*' | head -c 120)" | sed "s/${CLICKHOUSE_DATABASE}\.//"
        fi
    done
}

for table in s j a; do
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user"
    $CLICKHOUSE_CLIENT -q "GRANT SELECT(other) ON $CLICKHOUSE_DATABASE.$table TO $user"
    [ "$table" = a ] && $CLICKHOUSE_CLIENT -q "GRANT SELECT(other) ON $CLICKHOUSE_DATABASE.s TO $user"
    check "$table" "GRANT(other)"

    $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $CLICKHOUSE_DATABASE.$table TO $user"
    [ "$table" = a ] && $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $CLICKHOUSE_DATABASE.s TO $user"
    check "$table" "GRANT(other, p.s)"
done

$CLICKHOUSE_CLIENT -q "DROP USER $user"
