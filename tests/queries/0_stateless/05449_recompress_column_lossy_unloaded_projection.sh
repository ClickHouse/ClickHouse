#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-replicated-database, no-shared-merge-tree: the test drives two replicas of a
# `ReplicatedMergeTree` table into a specific fetch order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A replica that fetches parts before it applies `ADD PROJECTION` lists the projection in the
# checksums of these parts without loading it, and a mutation keeps such a projection. The
# execution-time lossy-codec guard of `RECOMPRESS COLUMN` must count it as present: otherwise
# `RECOMPRESS COLUMN val` queued while `val` is lossless and executed after `val` switched to
# `SZ3` rewrites `val` lossily while the kept projection describes the original values.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS r1 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS r2 SYNC"

for r in r1 r2
do
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE $r (key UInt64, val Float64 CODEC(ZSTD(1)), s UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_recompress_unloaded_projection', '$r')
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 max_postpone_time_for_failed_mutations_ms = 100"
done

${CLICKHOUSE_CLIENT} --query "INSERT INTO r1 VALUES (1000000, 0, 0)"
${CLICKHOUSE_CLIENT} --query "SYSTEM SYNC REPLICA r2"

# r2 cannot run the mutation of the data alter, so its ALTER that adds the projection waits behind it.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES r2"
${CLICKHOUSE_CLIENT} --alter_sync 1 --query "ALTER TABLE r1 MODIFY COLUMN s UInt64"
${CLICKHOUSE_CLIENT} --alter_sync 1 --query "ALTER TABLE r1 ADD PROJECTION p (SELECT key, val ORDER BY val)"

${CLICKHOUSE_CLIENT} --query "INSERT INTO r1 SELECT number, sin(number / 100.) * 100, 0 FROM numbers(1000)"

# r2 fetches the new part before it has the projection.
${CLICKHOUSE_CLIENT} --query "SYSTEM SYNC REPLICA r2 LIGHTWEIGHT"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES r2"
${CLICKHOUSE_CLIENT} --query "SYSTEM SYNC REPLICA r2"

# r2 executes the recompression itself.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES r1"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES r2"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE r2 RECOMPRESS COLUMN val"

# Metadata-only change; makes the queued recompression lossy.
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --alter_sync 1 --query \
    "ALTER TABLE r2 MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES r2"

# The mutation must fail with the dependency error, not rewrite the data.
fail_reason=""
for _ in {1..600}
do
    fail_reason=$(${CLICKHOUSE_CLIENT} --query "
        SELECT latest_fail_reason FROM system.mutations
        WHERE database = currentDatabase() AND table = 'r2' AND NOT is_done")
    if [[ "$fail_reason" == *"projection"* ]]; then break; fi
    sleep 0.3
done

echo "$fail_reason" | grep -o "Cannot RECOMPRESS COLUMN \`val\` with the lossy codec" | head -1
echo "$fail_reason" | grep -o "projection \`p\` reads this column" | head -1

# The stored values are still the original ones: the lossy rewrite did not run.
${CLICKHOUSE_CLIENT} --query "SELECT countIf(val = sin(key / 100.) * 100) FROM r2 WHERE key < 1000 SETTINGS optimize_use_projections = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE r1 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE r2 SYNC"
