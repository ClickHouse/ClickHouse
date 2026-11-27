#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# no-shared-merge-tree: it records even a metadata-only ALTER in system.mutations

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `ADD ENUM VALUES` through an Alias must keep the values that a concurrent ALTER of the target added.
function race()
{
    local via=$1
    # `al2` is created before `al`, because an Alias of an existing Alias is refused.
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS al2; DROP TABLE IF EXISTS al; DROP TABLE IF EXISTS tgt;
        CREATE TABLE tgt (x UInt64, e Enum16('v0' = 0)) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE al2 ENGINE = Alias('al');
        CREATE TABLE al ENGINE = Alias('tgt');"
    local through_alias="" direct=""
    for i in {1..20}; do
        through_alias+="ALTER TABLE $via MODIFY COLUMN e ADD ENUM VALUES ('a$i' = $i);"
        direct+="ALTER TABLE tgt MODIFY COLUMN e ADD ENUM VALUES ('b$i' = $((1000 + i)));"
    done
    $CLICKHOUSE_CLIENT -q "$through_alias" &
    $CLICKHOUSE_CLIENT -q "$direct" &
    wait
    # Every value is kept, and no ALTER rewrote the column.
    $CLICKHOUSE_CLIENT -q "
        SELECT '$via', countSubstrings(type, '\\'a'), countSubstrings(type, '\\'b') FROM system.columns
        WHERE database = currentDatabase() AND table = 'tgt' AND name = 'e';
        SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'tgt';"
}

# A concurrent ALTER of the target that drops values must not turn `ADD ENUM VALUES` through an Alias into a mutation.
function race_replace()
{
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS al2; DROP TABLE IF EXISTS al; DROP TABLE IF EXISTS tgt;
        CREATE TABLE tgt (x UInt64, e Enum16('v0' = 0)) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE al ENGINE = Alias('tgt');"
    local through_alias="" direct=""
    for i in {1..20}; do
        through_alias+="ALTER TABLE al MODIFY COLUMN e ADD ENUM VALUES ('a$i' = $((1000 + i)));"
        direct+="ALTER TABLE tgt MODIFY COLUMN e Enum16('v0' = 0, 'd$i' = $i);"
    done
    $CLICKHOUSE_CLIENT --enable_shared_storage_snapshot_in_query 1 -q "$through_alias" &
    $CLICKHOUSE_CLIENT -q "$direct" &
    wait
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = 'tgt' AND command LIKE '%ADD ENUM VALUES%';"
}

race al
race al2
race_replace
