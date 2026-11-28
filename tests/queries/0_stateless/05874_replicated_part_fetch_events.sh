#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel
# no-parallel: the profile events are global, so other tests must not fetch parts at the same time.

# `ReplicatedPartFailedFetches` counts the fetches that really failed,
# `ReplicatedPartFetchesNoSourceReplica` counts the fetches that found no active replica with the part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function event()
{
    $CLICKHOUSE_CLIENT -q "SELECT sumIf(value, name = '$1') FROM system.events"
}

function wait_for_failed_attempt()
{
    for _ in {1..600}
    do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 'r2' AND num_tries > 0") -gt 0 ]] && return
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY x;
    CREATE TABLE r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY x;
    SYSTEM STOP FETCHES r2;
"

echo "--- the source replica refuses to send the part"
failed_before=$(event ReplicatedPartFailedFetches)
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATED SENDS r1; INSERT INTO r1 VALUES (1); SYSTEM START FETCHES r2"
wait_for_failed_attempt
failed_after=$(event ReplicatedPartFailedFetches)
echo "failed fetches increased: $((failed_after > failed_before))"

echo "--- no active replica has the part"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP FETCHES r2; SYSTEM START REPLICATED SENDS r1; INSERT INTO r1 VALUES (2)"
no_source_before=$(event ReplicatedPartFetchesNoSourceReplica)
$CLICKHOUSE_CLIENT -q "DETACH TABLE r1; SYSTEM START FETCHES r2"
no_source_after=$no_source_before
for _ in {1..600}
do
    no_source_after=$(event ReplicatedPartFetchesNoSourceReplica)
    [[ $no_source_after -gt $no_source_before ]] && break
    sleep 0.1
done
echo "fetches without source replica increased: $((no_source_after > no_source_before))"

$CLICKHOUSE_CLIENT -q "ATTACH TABLE r1; DROP TABLE r2 SYNC; DROP TABLE r1 SYNC"
