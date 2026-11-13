#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro and IcebergLocal need USE_AVRO.

# A top-level Avro field `s.x` and the nested path `s` -> `x` have the same flattened name. Each column
# is read from one field: the top-level one if it exists, otherwise the first one in the schema.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DIR}"
trap 'rm -rf "${DIR}"' EXIT
mkdir -p "${DIR}"

echo "top-level field beside a tuple"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/s_first.avro', 'Avro', 'id Int32, s Tuple(x Int32), \`s.x\` Int32') SELECT 1, tuple(7), 99;
    INSERT INTO FUNCTION file('${DIR}/s_last.avro', 'Avro', 'id Int32, \`s.x\` Int32, s Tuple(x Int32)') SELECT 1, 99, tuple(7);
"
for f in s_first s_last; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT \`s.x\` FROM file('${DIR}/${f}.avro');
        SELECT id, \`s.x\` FROM file('${DIR}/${f}.avro', 'Avro', 'id Int32, \`s.x\` Int32');
        SELECT s, \`s.x\` FROM file('${DIR}/${f}.avro');
    "
done

echo "two nested paths"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/dotted_first.avro', 'Avro', 'a Tuple(\`b.c\` Int32, b Tuple(c Int32))') SELECT tuple(5, tuple(6));
    INSERT INTO FUNCTION file('${DIR}/dotted_last.avro', 'Avro', 'a Tuple(b Tuple(c Int32), \`b.c\` Int32)') SELECT tuple(tuple(6), 5);
"
for f in dotted_first dotted_last; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT \`a.b.c\` FROM file('${DIR}/${f}.avro', 'Avro', '\`a.b.c\` Int32');
        SELECT a.b.c FROM file('${DIR}/${f}.avro');
    "
done

echo "top-level field beside a nested array"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/n.avro', 'Avro', 'n Array(Tuple(a Int32)), \`n.a\` Array(Int32)') SELECT [tuple(1), tuple(2)], [9];
    SELECT \`n.a\` FROM file('${DIR}/n.avro', 'Avro', '\`n.a\` Array(Int32)');
"

echo "union branches with the same name"
${CLICKHOUSE_LOCAL} --input-format Avro -S 'id Int32, `u.R.x` Int32' --query 'SELECT * FROM table' < "${CURDIR}"/data_avro/union_same_name_record_branches.avro

echo "IcebergLocal"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --query "
    CREATE TABLE t (id Int32, s Tuple(x Nullable(Int32)), \`s.x\` Nullable(Int32)) ENGINE = IcebergLocal('${DIR}/ice/', 'Avro');
    INSERT INTO t VALUES (1, tuple(7), 99);
    SELECT \`s.x\` FROM t;
    SELECT id, \`s.x\` FROM t;
"
