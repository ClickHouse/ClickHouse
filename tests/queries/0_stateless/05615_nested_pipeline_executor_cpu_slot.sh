#!/usr/bin/env bash

# A processor that executes a pipeline, like `dictGet` on a `direct` dictionary, does it on the thread of the outer executor,
# which already holds a CPU slot. With `cpu_slot_preemption = false`, the nested executor waited forever for a second slot
# from a workload with `max_concurrent_threads = 1`, which is the slot held by the thread itself. Now its first thread runs
# on that slot. `cpu_slot_preemption` is a server setting, so the queries run in `clickhouse local` with its own config.
# https://github.com/ClickHouse/ClickHouse/issues/130300

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <cpu_slot_preemption>false</cpu_slot_preemption>
</clickhouse>
EOF

# A query that hangs is stopped by `timeout`, and the test fails on its missing results.
timeout 120 ${CLICKHOUSE_LOCAL} --config-file="${CONFIG}" --multiquery -q "
    SELECT name, value FROM system.server_settings WHERE name = 'cpu_slot_preemption';

    CREATE TABLE source (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE keys (key UInt64) ENGINE = MergeTree ORDER BY key;
    INSERT INTO source SELECT number, 1 FROM numbers(100);
    INSERT INTO keys SELECT number FROM numbers(100);

    CREATE DICTIONARY dict_direct (key UInt64, value UInt64) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(DIRECT());
    CREATE DICTIONARY dict_complex_key_direct (key UInt64, value UInt64) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(COMPLEX_KEY_DIRECT());
    -- Loaded before the workload exists: a dictionary is loaded on another thread, which is not the case tested here.
    CREATE DICTIONARY dict_hashed (key UInt64, value UInt64) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(HASHED()) LIFETIME(0);
    SYSTEM RELOAD DICTIONARY dict_hashed;

    CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD);
    CREATE WORKLOAD all;
    CREATE WORKLOAD one_slot IN all SETTINGS max_concurrent_threads = 1;

    SELECT 'direct', sum(dictGet('dict_direct', 'value', key)) FROM keys
    SETTINGS workload = 'one_slot', max_threads = 1, use_concurrency_control = 1;
    SELECT 'complex_key_direct', sum(dictGet('dict_complex_key_direct', 'value', tuple(key))) FROM keys
    SETTINGS workload = 'one_slot', max_threads = 1, use_concurrency_control = 1;
    SELECT 'direct, max_threads = 4', sum(dictGet('dict_direct', 'value', key)) FROM keys
    SETTINGS workload = 'one_slot', max_threads = 4, use_concurrency_control = 1;
    -- \`dictGetKeys\` reads the dictionary with several threads, one per block of 10 keys.
    SELECT 'dictGetKeys, max_threads = 4', length(dictGetKeys('dict_hashed', 'value', toUInt64(1))) FROM numbers(1)
    SETTINGS workload = 'one_slot', max_threads = 4, max_block_size = 10, use_concurrency_control = 1;
" 2>&1

rm -f "${CONFIG}"
