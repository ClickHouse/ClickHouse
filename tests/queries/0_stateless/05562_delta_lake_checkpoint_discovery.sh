#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Finding the checkpoint of a Delta Lake table without `_last_checkpoint` (it is optional), with a stale
# `_last_checkpoint`, and with the commits before the newest checkpoint removed by log cleanup, for classic,
# multi-part, and v2 (JSON and Parquet top-level file with sidecars) checkpoints written by Spark.
# A copy of each corpus table (see `05560_delta_lake_corpus`) is modified and read through the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_USER_FILES_UNIQUE}_checkpoints"
rm -rf "$BASE"
mkdir -p "$BASE"

# Prints the result of a query, or the name of its error code.
query()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --session_timezone UTC --query "$1" 2>&1); then
        echo "$out"
    else
        echo "$out" | grep -o -m1 -E '\([A-Z_]+\)$'
    fi
}

for table in deletion_vectors multi_part_checkpoint v2_checkpoint_json v2_checkpoint_parquet delta332_multi_part_checkpoint delta332_v2_checkpoint_parquet
do
    src="$CUR_DIR/data_lake_corpus/delta/$table"
    echo "-- $table"
    expected=$(${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(id) FROM deltaLakeLocal('$src')" | tr '\t' ' ')
    echo "original: $expected"
    checkpoints=$(find "$src/_delta_log" -maxdepth 1 -name '*.checkpoint*' -printf '%f\n' | cut -c1-20 | sort -u | sed 's/^0*//; s/^$/0/' | tr '\n' ' ')
    echo "checkpoints at versions: $checkpoints"
    newest=$(echo "$checkpoints" | awk '{ print $NF }')

    # No `_last_checkpoint`: the reader lists the log.
    dir="$BASE/$table.no_last_checkpoint"
    cp -r "$src" "$dir"
    rm "$dir/_delta_log/_last_checkpoint"
    echo "without _last_checkpoint: $(query "SELECT count(), sum(id) FROM deltaLakeLocal('$dir')" | tr '\t' ' ')"

    # The commits before the newest checkpoint are gone: the state comes from the checkpoint only.
    dir="$BASE/$table.cleaned_up"
    cp -r "$src" "$dir"
    for f in "$dir"/_delta_log/*.json "$dir"/_delta_log/*.crc
    do
        version=$(basename "$f" | cut -c1-20 | sed 's/^0*//; s/^$/0/')
        if [[ "$f" != *.checkpoint.* && "$version" -lt "$newest" ]]; then rm "$f"; fi
    done
    echo "cleaned up log: $(query "SELECT count(), sum(id) FROM deltaLakeLocal('$dir')" | tr '\t' ' ')"
    echo "cleaned up log, without _last_checkpoint: $(rm "$dir/_delta_log/_last_checkpoint"; query "SELECT count(), sum(id) FROM deltaLakeLocal('$dir')" | tr '\t' ' ')"
    echo "cleaned up log, version of the checkpoint: $(query "SELECT count() > 0 FROM deltaLakeLocal('$dir') SETTINGS delta_lake_snapshot_version = $newest")"
    echo "cleaned up log, version before the checkpoint: $(query "SELECT count() FROM deltaLakeLocal('$dir') SETTINGS delta_lake_snapshot_version = $((newest - 1))")"
done

# `_last_checkpoint` points to an older checkpoint: the reader still finds the commits after it.
dir="$BASE/stale_last_checkpoint"
cp -r "$CUR_DIR/data_lake_corpus/delta/deletion_vectors" "$dir"
echo '{"version":3,"size":4}' > "$dir/_delta_log/_last_checkpoint"
echo "-- stale _last_checkpoint: $(query "SELECT count(), sum(id) FROM deltaLakeLocal('$dir')" | tr '\t' ' ')"

rm -rf "$BASE"
