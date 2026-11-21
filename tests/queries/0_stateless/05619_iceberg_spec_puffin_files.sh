#!/usr/bin/env bash
# Tags: no-fasttest

# The Puffin files of the Apache Iceberg Java implementation (`core/src/test/resources/org/apache/iceberg/puffin/v1`)
# and the Puffin file with an LZ4-compressed footer of iceberg-go, see `data_iceberg/spec/README.md`.
# The files without blobs must be read with the blob metadata from their footers.
# `PuffinMetadata` reads only Puffin files of deletion vectors: the sample files of `TestPuffinReader`
# with the blobs `some-blob` and `some-other-blob` are rejected with `BAD_ARGUMENTS`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/spec"

for f in iceberg/puffin-v1/empty-puffin-uncompressed.bin iceberg-go/compressed-footer-lz4-cli.puffin
do
    echo "--- $f"
    $CLICKHOUSE_LOCAL --query "
    SELECT count() FROM file('$DATA/$f', PuffinMetadata);
    SELECT blob_type, snapshot_id, sequence_number, fields, offset, length, compression_codec, properties FROM file('$DATA/$f', PuffinMetadata) ORDER BY offset"
done

for f in iceberg/puffin-v1/sample-metric-data-uncompressed.bin iceberg/puffin-v1/sample-metric-data-compressed-zstd.bin
do
    echo "--- $f"
    $CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$DATA/$f', PuffinMetadata)" 2>&1 | grep -o "unsupported blob type 'some-blob'"
done

# A Puffin file without blobs has no deletion vectors.
$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$DATA/iceberg/puffin-v1/empty-puffin-uncompressed.bin', Puffin)"
