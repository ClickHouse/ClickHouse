#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: this test explicitly creates a Replicated database.

# Without `RANDOMIZE FOR`, a coordinated refreshable materialized view must still be refreshed by
# different replicas: each replica waits a random `refresh_coordination_jitter_ms` delay before
# racing for the refresh, so the same replica doesn't win every time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db1="${CLICKHOUSE_DATABASE}_r1"
db2="${CLICKHOUSE_DATABASE}_r2"
zk_path="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}/rmv_coordination_jitter"

# The refresh query runs with the view's database as the current database, so each row tells which replica refreshed.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    CREATE DATABASE ${db1} ENGINE = Replicated('${zk_path}', 's1', 'r1');
    CREATE DATABASE ${db2} ENGINE = Replicated('${zk_path}', 's1', 'r2');
    CREATE TABLE ${db1}.target (db String) ENGINE = ReplicatedMergeTree ORDER BY db;
    CREATE MATERIALIZED VIEW ${db1}.rmv REFRESH EVERY 1 SECOND APPEND TO ${db1}.target AS SELECT currentDatabase() AS db;"

# The initial refresh is due right away, so `SYSTEM WAIT VIEW` must wait for it through the random delay.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW ${db1}.rmv; SYSTEM SYNC REPLICA ${db1}.target; SELECT count() > 0 FROM ${db1}.target;"

# The chance that one replica wins every one of the ~60 refreshes is negligible.
for _ in {1..60}
do
    replicas=$($CLICKHOUSE_CLIENT -q "SELECT uniqExact(db) FROM ${db1}.target")
    [ "$replicas" -ge 2 ] && break
    sleep 1
done
echo "$replicas"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db2} SYNC; DROP DATABASE ${db1} SYNC;"
