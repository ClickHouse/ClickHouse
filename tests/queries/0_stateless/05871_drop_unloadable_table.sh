#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table whose loading fails at startup can still be dropped. Every clickhouse-local run below is a restart.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

local_run()
{
    ${CLICKHOUSE_LOCAL} --path "$data_dir" "$@"
}

local_run --query "
    CREATE DATABASE d;
    CREATE TABLE d.broken (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS object_shared_data_serialization_version = 'advanced';
    INSERT INTO d.broken VALUES (1);
    CREATE TABLE d.set_broken (x UInt64) ENGINE = Set SETTINGS persistent = 0;
    CREATE TABLE memory_broken (x UInt8) ENGINE = Memory SETTINGS max_rows_to_keep = 100;
"
uuid=$(local_run --query "SELECT uuid FROM system.tables WHERE database = 'd' AND name = 'broken'")

# A setting value that this version does not know makes the table fail to load.
sed -i "s/'advanced'/'from_a_newer_version'/" "$(find "$data_dir/store" -name 'broken.sql')"
sed -i "s/max_rows_to_keep = 100/max_rows_to_keep = 'from_a_newer_version'/" "$(find "$data_dir/store" -name 'memory_broken.sql')"
sed -i "s/persistent = 0/persistent = 'from_a_newer_version'/" "$(find "$data_dir/store" -name 'set_broken.sql')"

local_run --query "SELECT * FROM d.broken" 2>&1 | grep -o -m1 'ASYNC_LOAD_WAIT_FAILED'
local_run --query "SELECT * FROM memory_broken" 2>&1 | grep -o -m1 'ASYNC_LOAD_WAIT_FAILED'

local_run --query "DROP VIEW d.broken" 2>&1 | grep -o -m1 'is not a View'
local_run --query "SET max_table_size_to_drop = 1; DROP TABLE d.broken" 2>&1 | grep -o -m1 'TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT'

# Set does not support dropping without loading.
local_run --query "SET max_table_size_to_drop = 0; DROP TABLE d.set_broken SYNC" 2>&1 | grep -o -m1 'ASYNC_LOAD_WAIT_FAILED'
sed -i "s/persistent = 'from_a_newer_version'/persistent = 0/" "$(find "$data_dir/store" -name 'set_broken.sql')"

# The user may drop d.broken only.
users_xml="$(realpath "${CLICKHOUSE_TMP}")/users_${CLICKHOUSE_DATABASE}.xml"
cat > "$users_xml" <<'EOF'
<clickhouse>
    <profiles><default></default></profiles>
    <quotas><default></default></quotas>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <grants><query>GRANT DROP TABLE ON d.broken</query></grants>
        </default>
    </users>
</clickhouse>
EOF
local_run --query "DROP TABLE memory_broken SYNC" -- --users_config="$users_xml" 2>&1 | grep -o -m1 'ACCESS_DENIED'
local_run --query "SET max_table_size_to_drop = 0; DROP TABLE d.broken SYNC" -- --users_config="$users_xml"
local_run --query "DROP TABLE memory_broken SYNC"

test -d "$data_dir/store/${uuid:0:3}/$uuid" || echo 'data removed'
ls "$data_dir/metadata_dropped" | wc -l

local_run --query "
    CREATE TABLE d.broken (s String) ENGINE = Memory;
    CREATE TABLE memory_broken (x UInt8) ENGINE = Memory;
    DROP TABLE d.set_broken SYNC;
    SELECT database, name FROM system.tables WHERE name IN ('broken', 'memory_broken') ORDER BY name;
"

rm -rf "$data_dir" "$users_xml"
