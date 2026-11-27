#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The rewrite-rule matcher treats an equal tree hash as semantic equality. The tree hash of `INSERT`
# folded its children without their roles, so an `INSERT ... SELECT` and an `INSERT ... RETURNING (SELECT)`
# with the same `SELECT` hashed the same, and a rule for one of them also rejected the other.
# Only whether the rule fired is asserted, so the statements themselves need not succeed.
# Rule names are global; make them unique per test database so the test is parallel-safe.

RULE="rule_insert_roles_${CLICKHOUSE_DATABASE}"

check() # $1 query
{
    out=$($CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "$1" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$1: rejected"; else echo "$1: not rejected"; fi
}

pair() # $1 template, $2 other statement
{
    $CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS ($1) REJECT WITH 'blocked'"
    check "$1"
    check "$2"
    $CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
}

$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t (x UInt8) ENGINE = Memory"

pair "INSERT INTO t FROM INFILE 'nonexistent_file' SELECT 1" "INSERT INTO t RETURNING (SELECT 1) FROM INFILE 'nonexistent_file'"
pair "INSERT INTO t RETURNING (SELECT 1) FROM INFILE 'nonexistent_file'" "INSERT INTO t FROM INFILE 'nonexistent_file' SELECT 1"
