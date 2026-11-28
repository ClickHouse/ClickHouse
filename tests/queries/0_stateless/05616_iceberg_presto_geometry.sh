#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# Iceberg v3 `geometry` columns, top level and nested in `array`, `map` and `struct`, written by
# Presto. The tables and the expected results come from the Presto Iceberg connector tests
# (`TestGeometryTypeDataRead`, `TestGeometryArrayRead`, `TestGeometryMapRead`, `TestGeometryRowRead`),
# see `data_iceberg/presto/README.md`.
#
# The manifests of these tables have no `partition-spec`, `partition-spec-id` and `schema` keys
# in the Avro file header, so ClickHouse rejects them with `No partition-spec in iceberg manifest file`
# until https://github.com/ClickHouse/ClickHouse/issues/129989 is fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/presto"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`, so the tables
# are copied into a uniquely named subdirectory of the working directory.
WORK="./${CLICKHOUSE_TEST_UNIQUE_NAME}_presto"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT
WORK=$(realpath "$WORK")

# Presto's tests replace the `FILEPATH` placeholder in the metadata with the absolute path of
# a copy of the table; do the same, see `substitute_filepath.py`.
for table in geometry_data_type_read geometry_array geometry_map geometry_row
do
    python3 -I "$DATA/substitute_filepath.py" "$DATA/presto-iceberg/$table" "$WORK/$table"
done

$CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 -q "
SELECT 'geometry_data_type_read';
DESCRIBE icebergLocal('$WORK/geometry_data_type_read');
-- Every geometry kind: point, line string, polygon and their multi- versions.
SELECT variantType(geom), wkt(geom) FROM icebergLocal('$WORK/geometry_data_type_read') ORDER BY wkt(geom);
-- \`ST_Dimension(geom) = 0\` in Presto: points and multipoints.
SELECT count() FROM icebergLocal('$WORK/geometry_data_type_read') WHERE variantType(geom) IN ('Point', 'MultiPoint');

SELECT 'geometry_array';
DESCRIBE icebergLocal('$WORK/geometry_array');
SELECT length(geom), wkt(geom[1]), wkt(geom[2]), variantType(geom[3]), wkt(geom[3]) FROM icebergLocal('$WORK/geometry_array') ORDER BY ALL;

SELECT 'geometry_map';
DESCRIBE icebergLocal('$WORK/geometry_map');
SELECT name, wkt(mapgeometry['center']), wkt(mapgeometry['boundary']), mapKeys(mapgeometry) FROM icebergLocal('$WORK/geometry_map') ORDER BY ALL;
-- \`ST_YMin\` of the center point.
SELECT variantElement(mapgeometry['center'], 'Point').2 FROM icebergLocal('$WORK/geometry_map');

SELECT 'geometry_row';
DESCRIBE icebergLocal('$WORK/geometry_row');
SELECT num, name, wkt(geometryrow.geom1), wkt(geometryrow.geom2), geometryrow.flag FROM icebergLocal('$WORK/geometry_row') ORDER BY ALL;
"
