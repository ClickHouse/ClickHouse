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

rm -rf "${WORKING_FOLDER}"
