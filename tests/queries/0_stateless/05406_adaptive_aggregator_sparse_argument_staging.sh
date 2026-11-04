#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages the arguments of its missed rows. An aggregate of one argument takes
# a sparse column as it is, and the staging reads the values of the missed rows from it, without a dense copy of the
# column. Both tables below hold key 0 in the first 4096 rows of every granule, so the aggregation tables freeze
# holding key 0 after their first learning slice of 4096 rows. Each query reads its table twice, so that two threads
# aggregate. Each part runs in its own clickhouse-local process, so the counters in `system.events` belong to it alone.
# The settings that the freeze and the memory use depend on are pinned, and `max_threads_min_free_memory_per_thread = 0`
# keeps both threads whatever the free memory.
settings="
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192, preferred_block_size_bytes = 0;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;"

table_settings="ratio_of_defaults_for_sparse_serialization = 0.5, index_granularity = 8192, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0"

checks="
SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'records staged', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;"

# Every granule holds key 1 in its last row and a 16 MiB value in its row 4096, so the wide value is on a hit row, and
# every block stages one record, whose value is empty. Each thread holds its block with the wide value and the
# decompressed granule, so the query takes about 67 MiB. A dense copy of a block's column would hold the wide value once
# more, in a buffer of twice its size, and take the query to about 97 MiB. The query runs under a limit between the two
# amounts, with the feature off and on.
query="SELECT count(), sum(u) FROM (
    SELECT k, uniqExact(v) AS u FROM (SELECT * FROM t_wide UNION ALL SELECT * FROM t_wide) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
CREATE TABLE t_wide (n UInt64, k UInt64, v String) ENGINE = MergeTree ORDER BY n SETTINGS ${table_settings};
INSERT INTO t_wide SELECT number, number % 8192 = 8191, repeat(repeat('x', 1024), if(number % 8192 = 4096, 16384, 0))
FROM numbers(32768);
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wide' AND column = 'v' AND active;
${settings}
SET max_memory_usage = 83886080;
${query} SETTINGS enable_adaptive_aggregator = 0;
${query} SETTINGS enable_adaptive_aggregator = 1;
${checks}
"

# The other rows of every granule hold keys of their own, so every block stages 4096 records, in several batches, and
# one row in ten holds values other than the defaults. Sparse arguments are staged for `max` of a `String` and for `sum`
# of a `UInt64` and of a `Nullable(UInt64)`. `z` is staged from the dense column that the aggregation prepares for
# `argMax`, which reads it too. The results of every group are hashed and compared with the feature off.
query="SELECT count(), sum(cityHash64(k, ms, sx, sy IS NULL, ifNull(sy, 0), sz, az)) FROM (
    SELECT k, max(s) AS ms, sum(x) AS sx, sum(y) AS sy, sum(z) AS sz, argMax(z, n) AS az
    FROM (SELECT * FROM t_sparse UNION ALL SELECT * FROM t_sparse) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
CREATE TABLE t_sparse (n UInt64, k UInt64, s String, x UInt64, y Nullable(UInt64), z UInt64)
ENGINE = MergeTree ORDER BY n
SETTINGS ${table_settings}, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse';
INSERT INTO t_sparse SELECT number, if(number % 8192 < 4096, 0, number), if(number % 10 = 0, toString(number), ''),
    if(number % 10 = 0, number, 0), if(number % 10 = 0, number, NULL), if(number % 10 = 0, number, 0)
FROM numbers(65536);
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND column IN ('s', 'x', 'y', 'z') AND active ORDER BY column;
${settings}
${query} SETTINGS enable_adaptive_aggregator = 0;
${query} SETTINGS enable_adaptive_aggregator = 1;
${checks}
"
