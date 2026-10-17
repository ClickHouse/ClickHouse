#!/usr/bin/env bash
# Tags: no-fasttest, no-random-detach, no-replicated-database, no-ordinary-database
# no-fasttest: transactions are queried over the HTTP sessions of `transactions.lib`
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database
# no-ordinary-database: transactions need an `Atomic` database

# An open transaction keeps a table referenced through a mutation it created, even when the table has
# no parts for the mutation to process. The hook must not reattach such a table: the internal
# `DETACH TABLE ... SYNC` would wait for the transaction indefinitely.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_reattach_txn;
    CREATE TABLE t_reattach_txn (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
"

tx 1 "BEGIN TRANSACTION"
tx 1 "ALTER TABLE t_reattach_txn UPDATE x = x + 1 WHERE 1 SETTINGS alter_update_mode = 'heavy'"

check_if_not_detached "SELECT count() FROM t_reattach_txn" t_reattach_txn

tx 1 "ROLLBACK"

# Once the transaction is finished, the table is reattached again.
check_if_detached "SELECT count() FROM t_reattach_txn" t_reattach_txn

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_txn"
