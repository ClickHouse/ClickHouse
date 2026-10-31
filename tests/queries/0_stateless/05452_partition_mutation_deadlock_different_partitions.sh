#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database

# Deadlock detection between transactional and non-transactional mutations must not
# combine wait edges that go through different partitions. The transaction's earlier
# mutation affects partition 1, the intermediate non-transactional mutation is global,
# and the transaction's current mutation affects partition 2. The intermediate mutation
# waits for the transaction only in partition 1, and the current mutation waits for the
# intermediate one only in partition 2, where the intermediate mutation does not depend
# on the transaction, so there is no cycle and no `Deadlock detected` must be reported.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT -q "create table mt (p int, n int) engine=MergeTree partition by p order by tuple()"
$CLICKHOUSE_CLIENT -q "insert into mt values (1, 1), (2, 1)"

tx 1 "begin transaction"
# earlier mutation of the transaction, partition 1
tx 1 "alter table mt update n=n+10 in partition 1 where 1"
# intermediate non-transactional mutation, global; it is not executed until merges are started
$CLICKHOUSE_CLIENT -q "system stop merges mt"
$CLICKHOUSE_CLIENT -q "alter table mt update n=n+100 where 1"
# current mutation of the transaction, partition 2: it waits for the intermediate one
# in partition 2 only, so it must finish once merges are started
tx_async 1 "alter table mt update n=n+1 in partition 2 where 1"

# Let the deadlock detector see the current mutation while the intermediate one is still pending.
while [[ $($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 'mt'") -lt 3 ]]
do
    sleep 0.1
done
# The transactional `ALTER` checks the status of its mutation right after registering it.
sleep 0.5

$CLICKHOUSE_CLIENT -q "system start merges mt"
tx_wait 1
tx 1 "commit"

wait_for_all_mutations "mt"
$CLICKHOUSE_CLIENT -q "select p, n from mt order by p"
$CLICKHOUSE_CLIENT -q "drop table mt"
