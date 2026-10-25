#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-ordinary-database
# Tag no-fasttest: creates custom object-storage disks
# Tag no-replicated-database: every replica would run the ATTACH and mount the same disk root
# Tag no-shared-merge-tree: the parts found under the root are not the part set SharedMergeTree reads
# Tag no-ordinary-database: `lazy_load_tables` is a setting of an `Atomic` database

# A writable `table_disk` table registers itself as the owner of its disk root while it is being
# constructed, and a table attached over an overlapping root reads the `is_dropped` and `is_detached`
# flags of that owner. A table of a `lazy_load_tables = 1` database is constructed on the first access,
# and only then shares the lifecycle flags of the proxy that stood for it, so the flags were rebound while
# a concurrent `ATTACH` could be reading them (a data race found by TSan). Load such tables cold while
# another table is repeatedly attached over a root that overlaps theirs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY="${CLICKHOUSE_DATABASE}_lazy"
root="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
tables=4

function table_def()
{
    local name=$1 disk_name=$2 path=$3
    echo "$name (k Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS table_disk = true, disk = disk(
        name = '${disk_name}',
        type = object_storage, object_storage_type = local, metadata_type = plain_rewritable, path = '${path}')"
}

# A competitor is dropped only after the lazy database, whose drop removes the data on the shared roots.
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS \`${LAZY}\` SYNC" 2>/dev/null
    for i in $(seq 1 ${tables}); do
        ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE overlapping${i}" >/dev/null 2>&1
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS overlapping${i} SYNC" 2>/dev/null
    done
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE \`${LAZY}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1"
for i in $(seq 1 ${tables}); do
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE \`${LAZY}\`.$(table_def "t${i}" "${CLICKHOUSE_TEST_UNIQUE_NAME}_t${i}" "${root}/t${i}/")"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO \`${LAZY}\`.t${i} VALUES (${i})"
done

# Each table has a competitor over the same root of the same disk. A competitor only ever attaches over
# the existing data and is detached right after, never dropped, so it never writes to the root. Whichever
# side registers first wins and the other one is refused, so the outcome of every round is ignored, only
# the final state is checked.
declare -A competitor_uuid
for i in $(seq 1 ${tables}); do
    competitor_uuid[$i]=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")
done

function compete()
{
    local i=$1
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM \`${LAZY}\`.t${i} FORMAT Null" >/dev/null 2>&1 &
    for _ in $(seq 1 3); do
        # The first successful attach stores the definition, later ones reuse it.
        { ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE overlapping${i}" >/dev/null 2>&1 \
            || ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE $(table_def "overlapping${i} UUID '${competitor_uuid[$i]}'" "${CLICKHOUSE_TEST_UNIQUE_NAME}_t${i}" "${root}/t${i}/")" >/dev/null 2>&1; } \
            && ${CLICKHOUSE_CLIENT} --query "DETACH TABLE overlapping${i} SYNC"
    done
    wait
}

for _ in $(seq 1 10); do
    # Bring the tables back as proxies that have not been loaded.
    ${CLICKHOUSE_CLIENT} --query "DETACH DATABASE \`${LAZY}\`"
    ${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE \`${LAZY}\`"

    for i in $(seq 1 ${tables}); do
        compete "${i}" &
    done
    wait
done

${CLICKHOUSE_CLIENT} --query "DETACH DATABASE \`${LAZY}\`"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE \`${LAZY}\`"
for i in $(seq 1 ${tables}); do
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM \`${LAZY}\`.t${i}"
done
