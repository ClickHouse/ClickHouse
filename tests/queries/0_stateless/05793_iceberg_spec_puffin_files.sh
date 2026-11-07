#!/usr/bin/env bash
# Tags: no-fasttest

# The Puffin files of the Apache Iceberg Java implementation (`core/src/test/resources/org/apache/iceberg/puffin/v1`)
# and the Puffin file with an LZ4-compressed footer of iceberg-go, see `data_iceberg/spec/README.md`.
# The expected blob metadata comes from `TestPuffinReader` (the blob types, the fields, the offsets, the codecs)
# and from the footers; `PuffinMetadata` must list blobs of any type, not only deletion vectors.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/spec"

for f in iceberg/puffin-v1/empty-puffin-uncompressed.bin iceberg/puffin-v1/sample-metric-data-uncompressed.bin iceberg/puffin-v1/sample-metric-data-compressed-zstd.bin iceberg-go/compressed-footer-lz4-cli.puffin
do
    echo "--- $f"
    $CLICKHOUSE_LOCAL --query "
    SELECT count() FROM file('$DATA/$f', PuffinMetadata);
    SELECT blob_type, snapshot_id, sequence_number, fields, offset, length, compression_codec, properties FROM file('$DATA/$f', PuffinMetadata) ORDER BY offset"
done

# A Puffin file without blobs has no deletion vectors.
$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$DATA/iceberg/puffin-v1/empty-puffin-uncompressed.bin', Puffin)"
