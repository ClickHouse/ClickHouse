#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: the plain_merge_task_pause_before_prepare failpoint pauses every non-replicated merge of the
# server process while it is enabled, so this test must not run next to tests that merge.
# no-replicated-database, no-shared-merge-tree: the failpoint lives in the non-replicated merge path
# (MergePlainMergeTreeTask).
#
# The default codec of a merged part is resolved once, when the merge is selected and its memory reservation
# is priced, and the merge compresses with exactly that codec. A change of default_compression_codec while the
# selected merge waits must not switch it to a codec the reservation did not price (codecs differ in the
# scratch buffer they need per compressed block).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare" ||:
}
trap cleanup EXIT

# max_bytes_to_merge_at_max_space_in_pool = 1 keeps background selection away from the parts (OPTIMIZE FINAL
# ignores it), so the only merge ever selected is the one this test issues.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_merge_codec_frozen;
    CREATE TABLE t_merge_codec_frozen (k UInt64, v Float64) ENGINE = MergeTree ORDER BY k
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, default_compression_codec = 'LZ4';
    INSERT INTO t_merge_codec_frozen SELECT number, number / 3 FROM numbers(1000);
    INSERT INTO t_merge_codec_frozen SELECT number, number / 3 FROM numbers(1000, 1000);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare"

# The merge is selected here (resolving its codec with default_compression_codec = 'LZ4'), then parks on the
# failpoint before it starts executing.
$CLICKHOUSE_CLIENT --optimize_throw_if_noop 1 -q "OPTIMIZE TABLE t_merge_codec_frozen FINAL" &
optimize_pid=$!

selected=0
for _ in {1..600}
do
    selected=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_codec_frozen'")
    [ "$selected" -ge 1 ] && break
    sleep 0.1
done
if [ "$selected" -lt 1 ]
then
    echo "The merge was not selected in time"
fi

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_merge_codec_frozen MODIFY SETTING default_compression_codec = 'ZSTD(3)'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare"
wait $optimize_pid

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_merge_codec_frozen"
# The merged part is compressed with the codec resolved at selection.
$CLICKHOUSE_CLIENT -q "SELECT default_compression_codec FROM system.parts
    WHERE database = currentDatabase() AND table = 't_merge_codec_frozen' AND active"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_merge_codec_frozen"
