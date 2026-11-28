#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_puffin"
OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$OUT"

# A Theta blob without `properties`, with empty `properties` and with `ndv`, followed by the deletion vector written by Spark.
for theta_properties in '' ',\"properties\":{}' ',\"properties\":{\"ndv\":\"0\"}'
do
    echo "--- Theta properties: '${theta_properties//\\/}'"
    $CLICKHOUSE_LOCAL -q "
    WITH
        '{\"blobs\":[{\"type\":\"apache-datasketches-theta-v1\",\"fields\":[1],\"snapshot-id\":1,\"sequence-number\":1,\"offset\":4,\"length\":8${theta_properties}},'
        || '{\"type\":\"deletion-vector-v1\",\"fields\":[2147483645],\"snapshot-id\":-1,\"sequence-number\":-1,\"offset\":12,\"length\":58,'
        || '\"properties\":{\"referenced-data-file\":\"data/a.parquet\",\"cardinality\":\"5\"}}]}' AS footer,
        (SELECT substring(raw, 5, 58) FROM file('$DATA/spark_deletion_vector.puffin', RawBLOB, 'raw String')) AS deletion_vector
    SELECT concat('PFA1', unhex('01030300001E0000'), deletion_vector, 'PFA1', footer, reinterpretAsFixedString(toInt32(length(footer))), repeat('\\0', 4), 'PFA1')
    INTO OUTFILE '$OUT/theta.puffin' TRUNCATE FORMAT RawBLOB
    "
    $CLICKHOUSE_LOCAL -q "SELECT blob_type, fields, properties FROM file('$OUT/theta.puffin', PuffinMetadata) ORDER BY offset"
    $CLICKHOUSE_LOCAL -q "SELECT referenced_data_file, deleted_rows FROM file('$OUT/theta.puffin', Puffin)"
done

echo "--- ndv must be an unsigned integer when present"
$CLICKHOUSE_LOCAL -q "
WITH '{\"blobs\":[{\"type\":\"apache-datasketches-theta-v1\",\"fields\":[1],\"snapshot-id\":1,\"sequence-number\":1,\"offset\":4,\"length\":8,\"properties\":{\"ndv\":\"x\"}}]}' AS footer
SELECT concat('PFA1', unhex('01030300001E0000'), 'PFA1', footer, reinterpretAsFixedString(toInt32(length(footer))), repeat('\\0', 4), 'PFA1')
INTO OUTFILE '$OUT/bad_ndv.puffin' TRUNCATE FORMAT RawBLOB
"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$OUT/bad_ndv.puffin', PuffinMetadata)" 2>&1 | grep -o -m1 "property 'ndv' must be an unsigned integer"

rm -rf "${OUT:?}"
