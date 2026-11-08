#!/usr/bin/env bash
# Tags: no-fasttest

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dimensions"
mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR"' EXIT

python3 - "$DATA_DIR" <<'PY'
import json
import pathlib
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1])
for encoding in ("WKB", "WKT"):
    values = []
    for dimension, tag in ((1, "Z"), (2, "M"), (3, "ZM")):
        extra = [99.0, 101.0] if dimension == 3 else [99.0]
        if encoding == "WKB":
            header = struct.pack("<BII", 1, 1000 * dimension + 2, 2)
            coordinates = b"".join(struct.pack("<" + "d" * (2 + len(extra)), x, y, *extra) for x, y in ((1, 2), (4, 5)))
            values.append(header + coordinates)
        else:
            suffix = " ".join(str(x) for x in extra)
            values.append(f"LINESTRING {tag} (1 2 {suffix},4 5 {suffix})")
    table = pa.table({"id": [1, 2, 3], "geom": values})
    metadata = {
        "version": "1.1.0",
        "primary_column": "geom",
        "columns": {"geom": {"encoding": encoding, "geometry_types": []}},
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(metadata).encode()})
    pq.write_table(table, out / f"{encoding}.parquet")

features = []
for extra in ([], [99], [99, 101]):
    coordinates = [[1, 2] + extra, [4, 5] + extra]
    features.append({"type": "Feature", "id": str(len(features) + 1), "properties": {}, "geometry": {"type": "LineString", "coordinates": coordinates}})
(out / "geometry.json").write_text(json.dumps({"type": "FeatureCollection", "features": features}))
PY

for encoding in WKB WKT; do
    echo "$encoding"
    $CLICKHOUSE_LOCAL -q "SELECT id, geom, variantType(geom) FROM file('$DATA_DIR/$encoding.parquet', Parquet) ORDER BY id"
    $CLICKHOUSE_LOCAL -q "SELECT id, geom FROM file('$DATA_DIR/$encoding.parquet', Parquet, 'id Int64, geom LineString') ORDER BY id"
done

echo "GeoJSON"
$CLICKHOUSE_LOCAL -q "SELECT id, geometry FROM file('$DATA_DIR/geometry.json', GeoJSON) ORDER BY id"
