#!/usr/bin/env bash
# Tags: no-fasttest

# Deletion vectors of the Apache Iceberg Java implementation and Puffin files with deletion vectors
# of iceberg-go, see `data_iceberg/spec/README.md`.
#
# `iceberg/deletes/*-position-index.bin` are the deletion vector blobs that `TestBitmapPositionDeleteIndex`
# compares the Java serialization with, `iceberg/deletes/64map*.bin` are the portable 64-bit Roaring
# bitmaps of the Roaring format specification used by `TestRoaringPositionBitmap`. `wrap_dv.py` puts each
# of them into a Puffin file with the cardinality from the Java and iceberg-go tests, and the Puffin file
# is read with the `Puffin` format.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/spec"
WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

wrap()
{
    python3 "$DATA/iceberg/wrap_dv.py" "$DATA/iceberg/deletes/$1" "$WORK/$1.puffin" "file:///data/$1.parquet" "$2" ${3:-}
}

read_dv()
{
    echo "--- $1"
    $CLICKHOUSE_LOCAL --query "SELECT referenced_data_file, length(deleted_rows), deleted_rows FROM file('$WORK/$1.puffin', Puffin)"
}

# `TestBitmapPositionDeleteIndex`: positions 1, 3, 5, 7, 9.
wrap small-alternating-values-position-index.bin 5
read_dv small-alternating-values-position-index.bin

# `TestBitmapPositionDeleteIndex`: positions 100, 101, 2^31 - 1 + 100, 2^31 - 1 + 101.
wrap small-and-large-values-position-index.bin 4
read_dv small-and-large-values-position-index.bin

# `TestBitmapPositionDeleteIndex`: no positions.
wrap empty-position-index.bin 0
read_dv empty-position-index.bin

# `TestBitmapPositionDeleteIndex`: array, run and bitset containers in two 32-bit key buckets, 132561 positions
# (5, 7, [65537, 66536), [131073, 196607), 2^32 + 10, 2^32 + 20, [2^32 + 65546, 2^32 + 66036), [2^32 + 131073, 2^32 + 196607)).
wrap all-container-types-position-index.bin 132561
echo "--- all-container-types-position-index.bin"
$CLICKHOUSE_LOCAL --query "
SELECT length(deleted_rows), deleted_rows[1], deleted_rows[2], deleted_rows[3], arrayMax(deleted_rows), arraySum(deleted_rows),
    has(deleted_rows, 66535), has(deleted_rows, 66536), has(deleted_rows, 4294967306), has(deleted_rows, 4294967316),
    has(deleted_rows, 4294967296 + 196606), has(deleted_rows, 4294967296 + 196607)
FROM file('$WORK/all-container-types-position-index.bin.puffin', Puffin)"

# `TestRoaringPositionBitmap` and iceberg-go `roaring_bitmap_test.go`: the official Roaring examples.
# `64mapempty.bin`: no positions; `64map32bitvals.bin`: 0 to 9; `64mapspreadvals.bin`: 0 to 9 in each of the
# 32-bit key buckets 0 to 9.
wrap 64mapempty.bin 0 --roaring
read_dv 64mapempty.bin
wrap 64map32bitvals.bin 10 --roaring
read_dv 64map32bitvals.bin
wrap 64mapspreadvals.bin 100 --roaring
echo "--- 64mapspreadvals.bin"
$CLICKHOUSE_LOCAL --query "
SELECT length(deleted_rows), arraySum(deleted_rows),
    deleted_rows = arrayFlatten(arrayMap(i -> arrayMap(j -> bitShiftLeft(toUInt64(i), 32) + j, range(10)), range(10)))
FROM file('$WORK/64mapspreadvals.bin.puffin', Puffin)"

# `64maphighvals.bin` has 32-bit keys above the maximum supported position, it must be rejected
# (`TestRoaringPositionBitmap.testDeserializeUnsupportedRoaringExample`).
wrap 64maphighvals.bin 0 --roaring
echo "--- 64maphighvals.bin"
$CLICKHOUSE_LOCAL --query "SELECT length(deleted_rows) FROM file('$WORK/64maphighvals.bin.puffin', Puffin)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

# Puffin files written by Apache Iceberg Java for the iceberg-go cross-client tests (`dv_cross_client_test.go`)
# and by iceberg-go (`dv_golden_test.go`).
for f in single-blob-dv.puffin multi-blob-dv.puffin deletion-vector-v1.puffin
do
    echo "--- iceberg-go/$f"
    $CLICKHOUSE_LOCAL --query "
    SELECT blob_type, snapshot_id, sequence_number, fields, offset, length, compression_codec, properties FROM file('$DATA/iceberg-go/$f', PuffinMetadata) ORDER BY ALL;
    SELECT referenced_data_file, deleted_rows FROM file('$DATA/iceberg-go/$f', Puffin) ORDER BY ALL"
done
