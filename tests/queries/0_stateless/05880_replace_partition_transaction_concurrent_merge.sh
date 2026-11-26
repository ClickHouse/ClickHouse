#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# Looks like server does not listen https port in fasttest
# FIXME Replicated database executes ALTERs in separate context, so transaction info is lost

# A transactional REPLACE PARTITION must conflict with a transaction that removed the replaced parts
# (here by merging them) after its snapshot, like DROP PARTITION does, instead of keeping the old rows.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

# max_bytes_to_merge_at_max_space_in_pool = 0 disables background merges while leaving explicit OPTIMIZE working.
for t in dst dst2; do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $t (p UInt8, x UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY x
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, old_parts_lifetime = 10000"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${t/dst/src} AS $t"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $t SETTINGS async_insert = 0 VALUES (1, 1)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $t SETTINGS async_insert = 0 VALUES (1, 3)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${t/dst/src} SETTINGS async_insert = 0 VALUES (1, 2)"
done

# The concurrent merge is committed before the REPLACE.
tx 1 "begin transaction"
tx 1 "select 1, x from dst order by x"
tx 2                                            "begin transaction"
tx 2                                            "optimize table dst final settings optimize_throw_if_noop = 1"
tx 2                                            "commit"
tx 1 "alter table dst replace partition 1 from src" | grep -Eo "SERIALIZATION_ERROR" | uniq
tx 1 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 1 "rollback"
$CLICKHOUSE_CLIENT -q "select 2, x from dst order by x"

# The concurrent merge is still running (not committed) during the REPLACE.
tx 3 "begin transaction"
tx 3 "select 3, x from dst2 order by x"
tx 4                                            "begin transaction"
tx 4                                            "optimize table dst2 final settings optimize_throw_if_noop = 1"
tx 3 "alter table dst2 replace partition 1 from src2" | grep -Eo "SERIALIZATION_ERROR" | uniq
tx 3 "commit" | grep -Eo "INVALID_TRANSACTION" | uniq
tx 3 "rollback"
tx 4                                            "commit"
$CLICKHOUSE_CLIENT -q "select 4, x from dst2 order by x"

# Without a concurrent change the REPLACE succeeds; the parts outdated above are not visible to it.
tx 5 "begin transaction"
tx 5 "alter table dst replace partition 1 from src"
tx 5 "commit"
$CLICKHOUSE_CLIENT -q "select 5, x from dst order by x"

$CLICKHOUSE_CLIENT -q "DROP TABLE dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE src"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst2"
$CLICKHOUSE_CLIENT -q "DROP TABLE src2"
