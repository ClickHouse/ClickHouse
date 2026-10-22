#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
DATA_FILE="$TMP_DIR/geoparquet_orientation.parquet"

# Clockwise exterior rings are written as-is, not normalized to counterclockwise.
$CLICKHOUSE_LOCAL --query="
    SELECT
        [[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]::Polygon AS p,
        [[[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]]::MultiPolygon AS mp
    FORMAT Parquet" > "$DATA_FILE"

python3 - "$DATA_FILE" <<'PYEOF'
import json
import sys

import pyarrow.parquet as pq

metadata = pq.read_metadata(sys.argv[1]).metadata
assert metadata is not None and b"geo" in metadata
geo = json.loads(metadata[b"geo"])
for name, expected_type in (("p", "Polygon"), ("mp", "MultiPolygon")):
    column = geo["columns"][name]
    assert column["encoding"] == "WKB", column
    assert column["geometry_types"] == [expected_type], column
    assert column["edges"] == "planar", column
    assert "orientation" not in column, column
print("GeoParquet orientation metadata omitted")
PYEOF

# The metadata-only change must not modify the stored geometry.
$CLICKHOUSE_LOCAL --query="
    SELECT
        p = [[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]::Polygon,
        mp = [[[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]]::MultiPolygon
    FROM file('$DATA_FILE', Parquet)
    FORMAT TSV"
