#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Iceberg v3 stores a `variant` in Avro as a record of two `bytes` fields, `metadata` and `value`, in the
# variant binary encoding. Read as `Dynamic` or `JSON`, it gives the same values as a Parquet variant.

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

# 42, 'hello', {"age":30,"name":"Alice"}, [1,2], a variant null, NULL. The pattern matches no column, so
# String is written as Avro `bytes`.
${CLICKHOUSE_LOCAL} --output_format_avro_string_column_pattern='^never$' -q "
    CREATE VIEW raw AS SELECT * FROM values('id Int32, m String, v String',
        (1, '010000', '142A000000'),
        (2, '010000', '1568656C6C6F'),
        (3, '11020003076167656E616D65', '0202000100050B141E00000015416C696365'),
        (4, '010000', '03020002040C010C02'),
        (5, '010000', '00'),
        (6, '', ''));
    CREATE VIEW optional AS SELECT id, if(m = '', NULL, (unhex(m), unhex(v))::Tuple(metadata String, value String)) AS v FROM raw;
    INSERT INTO FUNCTION file('${DIR}/optional.avro', Avro) SELECT * FROM optional;
    INSERT INTO FUNCTION file('${DIR}/objects.avro', Avro) SELECT * FROM optional WHERE id IN (3, 5, 6);
    INSERT INTO FUNCTION file('${DIR}/required.avro', Avro) SELECT id, (unhex(m), unhex(v))::Tuple(metadata String, value String) AS v FROM raw WHERE id != 6;
    INSERT INTO FUNCTION file('${DIR}/swapped.avro', Avro) SELECT id, (unhex(v), unhex(m))::Tuple(value String, metadata String) AS v FROM raw WHERE id IN (1, 3);
    INSERT INTO FUNCTION file('${DIR}/nested.avro', Avro)
        SELECT [x, x] AS arr, map('k', x) AS m, (1, x)::Tuple(a Int32, v Tuple(metadata String, value String)) AS t
        FROM (SELECT (unhex(m), unhex(v))::Tuple(metadata String, value String) AS x FROM raw WHERE id = 3);
    INSERT INTO FUNCTION file('${DIR}/empty.avro', Avro) SELECT ('', '')::Tuple(metadata String, value String) AS v;
    INSERT INTO FUNCTION file('${DIR}/strings.avro', Avro) SELECT * FROM optional SETTINGS output_format_avro_string_column_pattern = '';
    INSERT INTO FUNCTION file('${DIR}/other.avro', Avro) SELECT (unhex(m), unhex(v))::Tuple(metadata String, other String) AS v FROM raw WHERE id = 1;
"

${CLICKHOUSE_LOCAL} -q "
    SELECT '-- Dynamic';
    SELECT id, v, dynamicType(v) FROM file('${DIR}/optional.avro', Avro, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- input_format_null_as_default = 0';
    SELECT id, v, dynamicType(v) FROM file('${DIR}/optional.avro', Avro, 'id Int32, v Dynamic') ORDER BY id SETTINGS input_format_null_as_default = 0;
    SELECT '-- a required record';
    SELECT id, v, dynamicType(v) FROM file('${DIR}/required.avro', Avro, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- value before metadata';
    SELECT id, v FROM file('${DIR}/swapped.avro', Avro, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- JSON';
    SELECT id, v FROM file('${DIR}/objects.avro', Avro, 'id Int32, v JSON') ORDER BY id;
    SELECT '-- Nullable(JSON)';
    SELECT id, v FROM file('${DIR}/objects.avro', Avro, 'id Int32, v Nullable(JSON)') ORDER BY id;
    SELECT '-- nested';
    SELECT * FROM file('${DIR}/nested.avro', Avro, 'arr Array(Dynamic), m Map(String, Dynamic), t Tuple(a Int32, v Dynamic)');
"

echo '-- errors'
${CLICKHOUSE_LOCAL} --ignore-error -q "
    SELECT * FROM file('${DIR}/optional.avro', Avro, 'id Int32, v JSON');
    SELECT * FROM file('${DIR}/empty.avro', Avro, 'v Dynamic');
    SELECT * FROM file('${DIR}/strings.avro', Avro, 'id Int32, v Dynamic');
    SELECT * FROM file('${DIR}/other.avro', Avro, 'v Dynamic');
" 2>&1 | grep -o \
    -e "Cannot read Parquet variant column 'v' as JSON: it contains a value of type Int32" \
    -e 'Malformed Parquet variant: 1 bytes are needed at offset 0, but the blob is 0 bytes' \
    -e 'Type Dynamic is not compatible with Avro record'
