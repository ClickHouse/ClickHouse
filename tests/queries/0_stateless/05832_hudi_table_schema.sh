#!/usr/bin/env bash
# Tags: no-fasttest

# The structure of a Hudi table. `hudi` takes it from the newest base file, and from the latest completed commit that has
# a schema when the table has no base file. The `Hudi` engine takes it from the query or from a base file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="05832_hudi_table_schema/${CLICKHOUSE_TEST_UNIQUE_NAME}"

ID='{"name":"id","type":["null","long"],"default":null}'
NAME='{"name":"name","type":["null","string"],"default":null}'
POINT='{"name":"point","type":["null",{"type":"record","name":"point","fields":[{"name":"x","type":"int"}]}],"default":null}'

function schema()
{
    local IFS=','
    echo "{\"type\":\"record\",\"name\":\"t\",\"fields\":[$*]}"
}

# Writes stdin to a file of the table.
function put()
{
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/$1', format='RawBLOB', structure='content String')
        SETTINGS s3_truncate_on_insert = 1
        FORMAT RawBLOB"
}

# Writes a completed commit with a schema, as JSON in timeline layout 1 and as Avro in timeline layout 2.
function put_commit()
{
    ${CLICKHOUSE_CLIENT} --param_schema "$3" -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/$1', format='$2')
        SETTINGS s3_truncate_on_insert = 1
        SELECT map('schema', {schema:String}) AS extraMetadata"
}

# Writes the rows of a query to a base file of the table.
function put_base_file()
{
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/$1')
        SETTINGS s3_truncate_on_insert = 1
        $2"
}

function describe()
{
    ${CLICKHOUSE_CLIENT} -q "
        DESCRIBE hudi(s3_conn, filename='${TABLE_PATH}/$1/')
        SETTINGS describe_compact_output = 1, print_pretty_type_names = 0"
}

# Reads a table that has a base file, then adds a newer base file that has one more column.
function add_column()
{
    local table=$1 older=$2 newer=$3
    printf 'hoodie.timeline.layout.version=1\n' | put "$table/.hoodie/hoodie.properties"
    put_commit "$table/.hoodie/20260101000000000.commit" JSONEachRow "$(schema "$ID")"
    put_base_file "$table/${older}_0-0-0_20260101000000000.parquet" "SELECT toInt64(1) AS id"
    describe "$table"
    put_commit "$table/.hoodie/20260102000000000.commit" JSONEachRow "$(schema "$ID" "$NAME")"
    put_base_file "$table/${newer}_0-0-0_20260102000000000.parquet" "SELECT toInt64(2) AS id, 'two' AS name"
    describe "$table"
    ${CLICKHOUSE_CLIENT} -q "SELECT id, name FROM hudi(s3_conn, filename='${TABLE_PATH}/$table/') ORDER BY id FORMAT CSV"
}

echo "A table without a base file has the columns of its latest commit and no row"
printf 'hoodie.timeline.layout.version=1\nhoodie.table.create.schema=%s\n' "$(schema "$ID")" | put empty/.hoodie/hoodie.properties
put_commit empty/.hoodie/20260101000000000.commit JSONEachRow "$(schema "$ID" "$NAME")"
echo '{}' | put empty/.hoodie/20260102000000000.commit
describe empty
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM hudi(s3_conn, filename='${TABLE_PATH}/empty/')"

echo "The engine is created on a table without a base file only with a structure"
# A table keeps the structure of its creation, so the engine does not take it from a commit.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE hudi_empty ENGINE = Hudi(s3_conn, filename='${TABLE_PATH}/empty/')" 2>&1 \
    | grep -o "CANNOT_EXTRACT_TABLE_STRUCTURE" | head -1
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE hudi_empty (id Int64) ENGINE = Hudi(s3_conn, filename='${TABLE_PATH}/empty/');
    SELECT count() FROM hudi_empty"
put_commit empty/.hoodie/20260103000000000.commit JSONEachRow "$(schema "$ID")"
put_base_file empty/a_0-0-0_20260103000000000.parquet "SELECT toInt64(7) AS id"
${CLICKHOUSE_CLIENT} -q "
    SELECT *, toTypeName(id) FROM hudi_empty;
    DROP TABLE hudi_empty"

echo "A table that was never written has the columns of hoodie.table.create.schema"
printf 'hoodie.timeline.layout.version=1\nhoodie.populate.meta.fields=false\nhoodie.table.create.schema=%s\n' "$(schema "$ID" "$NAME")" \
    | put created/.hoodie/hoodie.properties
describe created
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM hudi(s3_conn, filename='${TABLE_PATH}/created/')"

echo "The schema is the one of the latest completed commit that has one"
printf 'hoodie.timeline.layout.version=2\nhoodie.populate.meta.fields=false\n' | put commits/.hoodie/hoodie.properties
put_commit commits/.hoodie/timeline/20260101000000000_20260101000001000.commit Avro "$(schema "$ID")"
put_commit commits/.hoodie/timeline/20260102000000000_20260102000001000.commit Avro "$(schema "$ID" "$NAME")"
put_commit commits/.hoodie/timeline/20260103000000000_20260103000001000.commit Avro ''
put_commit commits/.hoodie/timeline/20260104000000000.commit.requested Avro "$(schema "$NAME")"
# Hudi reads an empty instant as a commit without metadata.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/commits/.hoodie/timeline/20260105000000000_20260105000001000.commit', format='RawBLOB')
    SETTINGS s3_truncate_on_insert = 1
    SELECT ''"
describe commits

echo "An optional struct of a table without a base file is not Nullable"
printf 'hoodie.timeline.layout.version=1\nhoodie.populate.meta.fields=false\n' | put struct/.hoodie/hoodie.properties
put_commit struct/.hoodie/20260101000000000.commit JSONEachRow "$(schema "$POINT")"
describe struct

echo "A column that only the newest base file has is not omitted"
# The newest base file is the first one by name in a table and the last one in the other.
add_column newest_is_first b a
add_column newest_is_last a b

echo "A table is not read as empty with another format than the one of its base files"
# Hudi reads the base file format from `hoodie.table.base.file.format`, then from its former name, and Parquet is the default.
for key in hoodie.table.base.file.format hoodie.table.ro.file.format
do
    printf 'hoodie.timeline.layout.version=1\n%s=ORC\n' "$key" | put "$key/.hoodie/hoodie.properties"
    put_commit "$key/.hoodie/20260101000000000.commit" JSONEachRow "$(schema "$ID")"
    echo 'not read' | put "$key/a_0-0-0_20260101000000000.orc"
    describe "$key" 2>&1 | grep -o "CANNOT_EXTRACT_TABLE_STRUCTURE" | head -1
done
${CLICKHOUSE_CLIENT} -q "DESCRIBE hudi(s3_conn, filename='${TABLE_PATH}/newest_is_last/', format='CSV')" 2>&1 \
    | grep -o "CANNOT_EXTRACT_TABLE_STRUCTURE" | head -1
