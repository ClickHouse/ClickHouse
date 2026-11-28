#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `persist_ttl_merge_times`, `merge_with_recompression_ttl_timeout` must survive a table restart and regular merges.

# CI randomizes the server default codec, and a part written with `ZSTD(3)` needs no recompression; pin it to `LZ4`.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_ttl_recompress_timeout;
    CREATE TABLE t_ttl_recompress_timeout (key UInt64, d DateTime)
    ENGINE = MergeTree ORDER BY key
    TTL d RECOMPRESS CODEC(ZSTD(3))
    SETTINGS persist_ttl_merge_times = 1, merge_with_recompression_ttl_timeout = 86400, max_number_of_merges_with_ttl_in_pool = 100,
        default_compression_codec = 'LZ4';
"

# A new part is written with the default codec and has an expired recompression TTL; wait until it is recompressed.
function insert_and_merge()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_ttl_recompress_timeout VALUES ($1, now() - 1000)"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_ttl_recompress_timeout SETTINGS optimize_throw_if_noop = 0"

    for _ in {1..600}; do
        local pending
        pending=$($CLICKHOUSE_CLIENT -q "
            SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_recompress_timeout'
                    AND active AND default_compression_codec != 'ZSTD(3)')
                + (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_ttl_recompress_timeout')")
        [ "$pending" -eq 0 ] && break
        sleep 0.1
    done
}

function ttl_recompress_merges()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS part_log;
        SELECT '$1', count() FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_ttl_recompress_timeout'
            AND event_type = 'MergeParts' AND merge_reason = 'TTLRecompressMerge' AND error = 0;
    "
}

insert_and_merge 1
ttl_recompress_merges 'first TTL merge'

$CLICKHOUSE_CLIENT -q "DETACH TABLE t_ttl_recompress_timeout"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_ttl_recompress_timeout"

# The partition is still postponed, so the new part is recompressed by a regular merge.
insert_and_merge 2
ttl_recompress_merges 'after restart'

$CLICKHOUSE_CLIENT -q "DETACH TABLE t_ttl_recompress_timeout"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_ttl_recompress_timeout"

insert_and_merge 3
ttl_recompress_merges 'after regular merge and restart'

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_ttl_recompress_timeout MODIFY SETTING merge_with_recompression_ttl_timeout = 0"

insert_and_merge 4
ttl_recompress_merges 'after timeout'

$CLICKHOUSE_CLIENT -q "SELECT key FROM t_ttl_recompress_timeout ORDER BY key"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ttl_recompress_timeout"
