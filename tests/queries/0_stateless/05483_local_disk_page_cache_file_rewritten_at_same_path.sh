#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache.yaml"

> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
"

# CODEC(NONE): both inserts write files of the same size. TRUNCATE + DETACH/ATTACH restarts block numbers, so the
# second insert writes all_1_1_0 again at the same paths. The sleeps let each version settle, so it is cached under
# its file version.
$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" \
    --use_page_cache_for_local_disks 1 \
    --local_filesystem_read_method pread \
    --multiquery "
CREATE DATABASE d ENGINE = Atomic;
CREATE TABLE d.t (x UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple() SETTINGS old_parts_lifetime = 0;
INSERT INTO d.t SELECT number + 1 FROM numbers(100);
SELECT sleep(3) FORMAT Null;
SELECT sum(x) FROM d.t;
TRUNCATE TABLE d.t;
DETACH TABLE d.t;
ATTACH TABLE d.t;
INSERT INTO d.t SELECT (number + 1) * 2 FROM numbers(100);
SELECT name FROM system.parts WHERE database = 'd' AND table = 't' AND active;
SELECT sum(x) FROM d.t;
SELECT sleep(3) FORMAT Null;
SELECT sum(x) FROM d.t;
SELECT sum(x) FROM d.t;
SELECT value > 0 FROM system.events WHERE event = 'PageCacheHits';
"

rm "${CONFIG_FILE}"
