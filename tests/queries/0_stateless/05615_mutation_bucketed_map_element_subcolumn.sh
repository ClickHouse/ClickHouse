#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --optimize_functions_to_subcolumns 1 --query "
    CREATE TABLE t (id UInt64, v UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
        map_buckets_strategy = 'constant', max_buckets_in_map = 16, map_buckets_min_avg_size = 0;
    INSERT INTO t SELECT number, 0, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(16)), arrayMap(i -> hex(randomString(30)), range(16))) FROM numbers(20000);

    CREATE TABLE read_bytes (step String, bytes UInt64) ENGINE = Memory;
    INSERT INTO read_bytes SELECT 'start', sum(value) FROM system.events WHERE event = 'ReadCompressedBytes';
    ALTER TABLE t UPDATE v = v + 1 WHERE m.key_k1 = 'nomatch' SETTINGS mutations_sync = 1;
    INSERT INTO read_bytes SELECT 'subcolumn', sum(value) FROM system.events WHERE event = 'ReadCompressedBytes';
    ALTER TABLE t UPDATE v = v + 1 WHERE m['k1'] = 'nomatch' SETTINGS mutations_sync = 1;
    INSERT INTO read_bytes SELECT 'element', sum(value) FROM system.events WHERE event = 'ReadCompressedBytes';
    DELETE FROM t WHERE m['k1'] = 'nomatch';
    INSERT INTO read_bytes SELECT 'delete', sum(value) FROM system.events WHERE event = 'ReadCompressedBytes';

    WITH
        (SELECT bytes FROM read_bytes WHERE step = 'start') AS b0,
        (SELECT bytes FROM read_bytes WHERE step = 'subcolumn') AS b1,
        (SELECT bytes FROM read_bytes WHERE step = 'element') AS b2,
        (SELECT bytes FROM read_bytes WHERE step = 'delete') AS b3
    SELECT b1 - b0 > 0, b2 - b1 <= 2 * (b1 - b0), b3 - b2 <= 2 * (b1 - b0);
    SELECT count(), sum(v) FROM t;
"
