#!/usr/bin/env bash

# `CHECK` constraints that DDL rejects but stored metadata still has: the table loads, and each
# constraint is reported when a row is inserted. The metadata is written by hand, as in
# `05072_check_constraint_row_count_mismatch`.

# Creation of a database with the Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/05875_check_constraint_analyzer_stored_metadata"

# Prints the error name of the `INSERT` of row (1, 2), or the rows of the table when it succeeds.
check()
{
    rm -rf "${WORKING_FOLDER}"
    mkdir -p "${WORKING_FOLDER}/metadata/stored"
    echo "ATTACH DATABASE stored ENGINE = Ordinary" > "${WORKING_FOLDER}/metadata/stored.sql"
    echo "ATTACH TABLE stored.o (id Int32) ENGINE = MergeTree ORDER BY tuple();" > "${WORKING_FOLDER}/metadata/stored/o.sql"
    echo "ATTACH TABLE stored.t (x Int32, y Int32, CONSTRAINT c CHECK $2) ENGINE = MergeTree ORDER BY tuple();" > "${WORKING_FOLDER}/metadata/stored/t.sql"
    echo -n "$1: "
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES (1, 2); SELECT * FROM stored.t" 2>&1 \
        | grep -o -m1 -E '\(([A-Z_]+)\)|^1	2$'
}

check "window" "row_number() OVER () > 0"
check "grouping" "grouping(x) = 0"
check "matcher" "tuple(*) != (0, 0)"
check "correlated exists" "exists(SELECT 1 FROM stored.o WHERE id = x)"
check "correlated IN" "x IN (SELECT id + y FROM stored.o)"
check "alias" "(x + 1 AS z) > 0 AND z < 10"

# Prints the violated constraint of the `INSERT` of row $3 into a table with constraints $2, or the row when it succeeds.
check_arr()
{
    rm -rf "${WORKING_FOLDER}"
    mkdir -p "${WORKING_FOLDER}/metadata/stored"
    echo "ATTACH DATABASE stored ENGINE = Ordinary" > "${WORKING_FOLDER}/metadata/stored.sql"
    echo "ATTACH TABLE stored.t (x Int32, arr Array(Int32), $2) ENGINE = MergeTree ORDER BY tuple();" > "${WORKING_FOLDER}/metadata/stored/t.sql"
    echo -n "$1, row $3: "
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "INSERT INTO stored.t VALUES $3; SELECT 'ACCEPTED', * FROM stored.t" 2>&1 \
        | grep -o -E 'Constraint `[a-z0-9]+`|\(([A-Z_]+)\)|^ACCEPTED.*' | head -n2 | tr '\t\n' '  ' | sed 's/ $//'
    echo
}

# A lambda argument named as a column, the column itself referred to by an alias.
c1="CONSTRAINT c1 CHECK (x AS bound) >= 0 AND arrayAll(x -> x >= bound, arr)"
c2="CONSTRAINT c2 CHECK (x AS b2) >= 0 AND arrayExists(x -> x = b2, arr)"
check_arr "lambda argument x" "$c1" "(10, [1])"
check_arr "lambda argument x" "$c1" "(1, [10])"
check_arr "two constraints" "$c1, $c2" "(5, [5, 7])"
check_arr "two constraints" "$c1, $c2" "(5, [6, 7])"

rm -rf "${WORKING_FOLDER}"
