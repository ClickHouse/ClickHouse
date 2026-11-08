#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Iceberg v3 stores a `variant` in ORC as a struct of two binary fields, `metadata` and `value`, in the
# variant binary encoding. Read as `Dynamic` or `JSON`, it gives the same values as a Parquet variant.

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

# 42, 'hello', {"age":30,"name":"Alice"}, [1,2], a variant null, NULL. Strings are written as ORC binary.
${CLICKHOUSE_LOCAL} --output_format_orc_string_as_string=0 -q "
    CREATE VIEW raw AS SELECT * FROM values('id Int32, m String, v String',
        (1, '010000', '142A000000'),
        (2, '010000', '1568656C6C6F'),
        (3, '11020003076167656E616D65', '0202000100050B141E00000015416C696365'),
        (4, '010000', '03020002040C010C02'),
        (5, '010000', '00'),
        (6, '', ''));
    CREATE VIEW optional AS SELECT id, if(m = '', NULL, (unhex(m), unhex(v))::Tuple(metadata String, value String)) AS v FROM raw;
    INSERT INTO FUNCTION file('${DIR}/optional.orc', ORC) SELECT * FROM optional;
    INSERT INTO FUNCTION file('${DIR}/objects.orc', ORC) SELECT * FROM optional WHERE id IN (3, 5, 6);
    INSERT INTO FUNCTION file('${DIR}/required.orc', ORC) SELECT id, (unhex(m), unhex(v))::Tuple(metadata String, value String) AS v FROM raw WHERE id != 6;
    INSERT INTO FUNCTION file('${DIR}/swapped.orc', ORC) SELECT id, (unhex(v), unhex(m))::Tuple(value String, metadata String) AS v FROM raw WHERE id IN (1, 3);
    INSERT INTO FUNCTION file('${DIR}/nested.orc', ORC)
        SELECT [x, x] AS arr, map('k', x) AS m, (1, x)::Tuple(a Int32, v Tuple(metadata String, value String)) AS t
        FROM (SELECT (unhex(m), unhex(v))::Tuple(metadata String, value String) AS x FROM raw WHERE id = 3);
    INSERT INTO FUNCTION file('${DIR}/outer.orc', ORC)
        SELECT id, if(id = 1, NULL, (id, (unhex(m), unhex(v))::Tuple(metadata String, value String)))::Nullable(Tuple(a Int32, v Tuple(metadata String, value String))) AS s
        FROM raw WHERE id IN (1, 3);
    INSERT INTO FUNCTION file('${DIR}/empty.orc', ORC) SELECT ('', '')::Tuple(metadata String, value String) AS v;
    INSERT INTO FUNCTION file('${DIR}/one_sided.orc', ORC) SELECT (NULL, unhex('142A000000'))::Tuple(metadata Nullable(String), value Nullable(String)) AS v;
    INSERT INTO FUNCTION file('${DIR}/strings.orc', ORC) SELECT * FROM optional SETTINGS output_format_orc_string_as_string = 1;
    INSERT INTO FUNCTION file('${DIR}/other.orc', ORC) SELECT (unhex(m), unhex(v))::Tuple(metadata String, other String) AS v FROM raw WHERE id = 1;
"

${CLICKHOUSE_LOCAL} -q "
    SELECT '-- Dynamic';
    SELECT id, v, dynamicType(v) FROM file('${DIR}/optional.orc', ORC, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- input_format_null_as_default = 0';
    SELECT id, v, dynamicType(v) FROM file('${DIR}/optional.orc', ORC, 'id Int32, v Dynamic') ORDER BY id SETTINGS input_format_null_as_default = 0;
    SELECT '-- a struct without nulls';
    SELECT id, v FROM file('${DIR}/required.orc', ORC, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- value before metadata';
    SELECT id, v FROM file('${DIR}/swapped.orc', ORC, 'id Int32, v Dynamic') ORDER BY id;
    SELECT '-- JSON';
    SELECT id, v FROM file('${DIR}/objects.orc', ORC, 'id Int32, v JSON') ORDER BY id;
    SELECT '-- Nullable(JSON)';
    SELECT id, v FROM file('${DIR}/objects.orc', ORC, 'id Int32, v Nullable(JSON)') ORDER BY id;
    SELECT '-- nested';
    SELECT * FROM file('${DIR}/nested.orc', ORC, 'arr Array(Dynamic), m Map(String, Dynamic), t Tuple(a Int32, v Dynamic)');
    SELECT '-- in a nullable struct';
    SELECT * FROM file('${DIR}/outer.orc', ORC, 'id Int32, s Tuple(a Nullable(Int32), v Dynamic)') ORDER BY id;
    SELECT '-- as a Tuple';
    SELECT id, hex(v.metadata), hex(v.value) FROM file('${DIR}/required.orc', ORC, 'id Int32, v Tuple(metadata String, value String)') WHERE id = 1;
"

echo '-- errors'
${CLICKHOUSE_LOCAL} --ignore-error -q "
    SELECT * FROM file('${DIR}/optional.orc', ORC, 'id Int32, v JSON');
    SELECT * FROM file('${DIR}/empty.orc', ORC, 'v Dynamic');
    SELECT * FROM file('${DIR}/one_sided.orc', ORC, 'v Dynamic');
    SELECT * FROM file('${DIR}/strings.orc', ORC, 'id Int32, v Dynamic');
    SELECT * FROM file('${DIR}/other.orc', ORC, 'v Dynamic');
    SELECT * FROM file('${DIR}/other.orc', ORC, 'v JSON');
" 2>&1 | grep -o \
    -e "Cannot read Parquet variant column 'v' as JSON: it contains a value of type Int32" \
    -e "Malformed ORC variant column 'v': a row has an empty \`metadata\`" \
    -e "Malformed Parquet variant column 'v': a row has \`value\` but no \`metadata\`" \
    -e "Cannot read ORC column 'v' of type [^ ]* as [A-Za-z]*"
