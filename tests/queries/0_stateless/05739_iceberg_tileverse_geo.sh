#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Geospatial Iceberg tables of the iceberg-geo-testbed (written with PyIceberg): 10 disjoint regions x 1000 points.
# The tables come from the parquetry test kit, see `data_iceberg/tileverse/README.md`.
# The metadata is rooted at `file:///iceberg-<corpus>/<table>`, which is resolved relative to the table directory;
# the queries run with `data_iceberg/tileverse` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1

G="iceberg-geo-testbed"

# The expected counts are from the testbed: 10000 rows in every table, and the California probe
# (`engines/duckdb/run.py`) finds 196 rows. The other values are cross-checked between the tables.
$CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
SELECT 'counts';
SELECT count() FROM icebergLocal('$G/v2_flat_columns');
SELECT count() FROM icebergLocal('$G/v2_bbox_struct');
SELECT count() FROM icebergLocal('$G/v2_geo_convention');
SELECT count() FROM icebergLocal('$G/v3_geometry');
SELECT count() FROM icebergLocal('$G/v3_geometry_lineage');
SELECT count() FROM icebergLocal('$G/v3_minimal');
SELECT splitByChar('-', id)[1] AS region, count(), round(min(xmin), 3), round(max(xmax), 3), round(min(ymin), 3), round(max(ymax), 3)
    FROM icebergLocal('$G/v2_flat_columns') GROUP BY region ORDER BY region;

-- The California probe: flat bbox columns, a bbox struct, GeoIceberg V2 convention columns, native geometry points.
SELECT 'probe';
SELECT count() FROM icebergLocal('$G/v2_flat_columns') WHERE xmin <= -118 AND xmax >= -125 AND ymin <= 40 AND ymax >= 37;
SELECT count() FROM icebergLocal('$G/v2_bbox_struct') WHERE bbox.xmin <= -118 AND bbox.xmax >= -125 AND bbox.ymin <= 40 AND bbox.ymax >= 37;
SELECT count() FROM icebergLocal('$G/v2_geo_convention') WHERE fp_xmin <= -118 AND fp_xmax >= -125 AND fp_ymin <= 40 AND fp_ymax >= 37;
SELECT count() FROM icebergLocal('$G/v3_geometry') WHERE geom.Point.1 BETWEEN -125 AND -118 AND geom.Point.2 BETWEEN 37 AND 40;
SELECT count() FROM icebergLocal('$G/v3_geometry') WHERE geom.Point.1 BETWEEN -125 AND -115 AND geom.Point.2 BETWEEN 32 AND 42;

-- \`geometry(OGC:CRS84)\` columns read as \`Geometry\`; the points are the lower corners of the bboxes of the V2 tables
-- and the WKB of the GeoIceberg V2 convention table.
SELECT 'geometry';
SELECT DISTINCT toTypeName(geom), variantType(geom) FROM icebergLocal('$G/v3_geometry');
SELECT id, geom FROM icebergLocal('$G/v3_geometry') WHERE id IN ('california-0', 'uk-999') ORDER BY id;
SELECT id, readWKB(geom_wkb) FROM icebergLocal('$G/v2_geo_convention') WHERE id IN ('california-0', 'uk-999') ORDER BY id;
SELECT count() FROM icebergLocal('$G/v3_geometry') AS g INNER JOIN icebergLocal('$G/v2_flat_columns') AS f USING (id)
    WHERE g.geom.Point.1 = f.xmin AND g.geom.Point.2 = f.ymin;
SELECT count() FROM icebergLocal('$G/v3_geometry_lineage') AS g INNER JOIN icebergLocal('$G/v2_geo_convention') AS f USING (id)
    WHERE g.geom = readWKB(f.geom_wkb);

-- The lineage table materializes \`_row_id\` and \`_last_updated_sequence_number\` in the data files.
SELECT 'lineage';
SELECT id, _row_id, _last_updated_sequence_number FROM icebergLocal('$G/v3_geometry_lineage') WHERE id IN ('california-0', 'california-1') ORDER BY id;
SELECT min(_row_id), max(_row_id), uniqExact(_row_id), groupUniqArray(_last_updated_sequence_number) FROM icebergLocal('$G/v3_geometry_lineage');

-- The smallest V3 table, no geometry: \`n\` is pseudo-random (the sum is the same with PyIceberg).
SELECT 'v3_minimal';
SELECT count(), uniqExact(id), sum(n) FROM icebergLocal('$G/v3_minimal');
"
