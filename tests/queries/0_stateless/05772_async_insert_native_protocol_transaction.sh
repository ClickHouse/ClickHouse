#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"
printf '1\n2\n3\n' > "$DATA"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t_async (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS async_insert = 1;
"

# FROM INFILE data reaches the server as native blocks, not as inline query data.
$CLICKHOUSE_CLIENT -q "
    SET async_insert = 1, wait_for_async_insert = 1;
    BEGIN TRANSACTION;
    INSERT INTO t FROM INFILE '$DATA' FORMAT TSV; -- { serverError NOT_IMPLEMENTED }
    ROLLBACK;
    SELECT count() FROM t;
"

$CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 --implicit_transaction 1 \
    -q "INSERT INTO t FROM INFILE '$DATA' FORMAT TSV" 2>&1 \
    | grep -o -m1 "Async inserts with 'implicit_transaction' are not supported"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"

# With async_insert = 0 on the client, it parses VALUES itself and sends a block; the table setting enables the async insert.
$CLICKHOUSE_CLIENT -q "
    SET async_insert = 0, send_table_structure_on_insert_with_inline_data = 1;
    BEGIN TRANSACTION;
    INSERT INTO t_async VALUES (1); -- { serverError NOT_IMPLEMENTED }
    ROLLBACK;
    SELECT count() FROM t_async;
"

# Opting out of the exception runs the insert synchronously inside the transaction.
$CLICKHOUSE_CLIENT -q "
    SET async_insert = 1, wait_for_async_insert = 1, throw_on_unsupported_query_inside_transaction = 0;
    BEGIN TRANSACTION;
    INSERT INTO t FROM INFILE '$DATA' FORMAT TSV;
    SELECT count() FROM t;
    ROLLBACK;
    SELECT count() FROM t;
"
