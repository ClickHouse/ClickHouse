#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-fasttest, long
# no-shared-merge-tree: custom disk
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/132006:
# the access check of a disk created by `disk(...)` in CREATE TABLE must not run under `storage_policies_mutex`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 10.255.255.1 does not answer, so the access check of the new disk hangs until `max_execution_time`.
query_id="${CLICKHOUSE_DATABASE}_hanging_disk_create"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --max_execution_time 15 -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_hanging_disk (n UInt8) ENGINE = MergeTree ORDER BY n
    SETTINGS disk = disk(type = 's3', endpoint = 'http://10.255.255.1:9/bucketname/', access_key_id = 'a', secret_access_key = 'b')
" >/dev/null 2>&1 &
create_pid=$!

# Wait until the CREATE TABLE is running, then give it a moment to reach the access check.
for _ in {1..100}
do
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" = "1" ] && break
    sleep 0.1
done
sleep 1

# These need `storage_policies_mutex`. Before the fix they waited for the whole `max_execution_time` of the query above.
start=$(date +%s)
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.storage_policies"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.disks"
end=$(date +%s)

# The CREATE TABLE is still running here (it needs ~15 s), the queries above must not have waited for it.
if kill -0 $create_pid 2>/dev/null; then echo "create still running"; else echo "create finished too early"; fi
if [ $((end - start)) -lt 8 ]; then echo "not blocked"; else echo "blocked for $((end - start)) s"; fi

wait $create_pid
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_hanging_disk"
