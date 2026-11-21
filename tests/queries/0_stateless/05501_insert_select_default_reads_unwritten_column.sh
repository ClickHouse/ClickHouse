#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs MinIO

# `INSERT INTO t SELECT ... FROM <table function>` fills the fields missing in the input with the `DEFAULT`s of `t`.
# A `DEFAULT` that reads a column the `INSERT` does not write (`ALIAS`, `MATERIALIZED`, `EPHEMERAL`, or a column left out
# of the column list) must give the value of a direct insert into `t`. Each case prints the rows of the direct insert
# and of the `INSERT SELECT` from `file`, `s3Cluster` and `input`, which must all be equal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05501"
FILE_DIR="${CLICKHOUSE_DATABASE}_05501"
CLUSTER="test_cluster_one_shard_three_replicas_localhost"

# The first row of each input lacks the defaulted fields, the second one has them.
declare -A INPUT=(
    [rows]='{"id":1}
{"id":2,"q":100}'
    [array_rows]='{"id":1}
{"id":2,"q":[100]}'
    [column_list_rows]='{"id":1}
{"id":2,"q_od":100,"q_nd":200}'
    [x_rows]='{"id":1,"x":100}'
)

SETUP="
    CREATE TABLE kv_source (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO kv_source VALUES (1, 42);
    CREATE DICTIONARY kv_dictionary (k UInt32, v UInt32) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'kv_source' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT()) LIFETIME(0);"
for input in "${!INPUT[@]}"; do
    SETUP+="
        INSERT INTO FUNCTION s3('${S3_DIR}/${input}.jsonl', 'RawBLOB', 'raw String') SETTINGS s3_truncate_on_insert = 1
        VALUES ('${INPUT[$input]}');
        INSERT INTO FUNCTION file('${FILE_DIR}/${input}.jsonl', 'RawBLOB', 'raw String') SETTINGS engine_file_truncate_on_insert = 1
        VALUES ('${INPUT[$input]}');"
done

NAMES=()
declare -A INPUT_OF INSERT_COLUMNS SELECT_LIST DIRECT_ROWS
FILE_INSERTS=""
S3CLUSTER_INSERTS=""
PRINT=""

# check <name> <input> <columns> <INSERT column list> <SELECT list> [<rows of the direct insert>]
function check()
{
    local name=$1
    NAMES+=("${name}")
    INPUT_OF[${name}]=$2
    INSERT_COLUMNS[${name}]=$4
    SELECT_LIST[${name}]=$5
    DIRECT_ROWS[${name}]=${6:-${INPUT[$2]}}

    SETUP+="
        CREATE TABLE ${name}_direct ($3) ENGINE = MergeTree ORDER BY tuple();
        CREATE TABLE ${name}_file AS ${name}_direct;
        CREATE TABLE ${name}_s3cluster AS ${name}_direct;
        CREATE TABLE ${name}_input AS ${name}_direct;"
    FILE_INSERTS+="
        INSERT INTO ${name}_file $4 SELECT $5 FROM file('${FILE_DIR}/$2.jsonl', 'JSONEachRow');"
    S3CLUSTER_INSERTS+="
        INSERT INTO ${name}_s3cluster $4 SELECT $5 FROM s3Cluster('${CLUSTER}', '${S3_DIR}/$2.jsonl', 'JSONEachRow')
        SETTINGS parallel_distributed_insert_select = 0;"
    for path in direct file s3cluster input; do
        PRINT+="SELECT '${name}', '${path}', * FROM ${name}_${path} ORDER BY ALL;"
    done
}

check alias rows "id UInt32, al UInt32 ALIAS id + 1, q UInt32 DEFAULT al + 1" "" "*"
check alias_of_alias rows "id UInt32, a UInt32 ALIAS id + 1, b UInt32 ALIAS a * 10, q UInt32 DEFAULT b + 1" "" "*"
check narrow_alias rows "id UInt32, al UInt8 ALIAS id + 300, q UInt32 DEFAULT al" "" "*"
check materialized rows "id UInt32, m UInt32 MATERIALIZED id + 100, q UInt32 DEFAULT m + 1" "" "*"
check ephemeral rows "id UInt32, e UInt32 EPHEMERAL id + 100, q UInt32 DEFAULT e + 1" "" "*"
check lambda_body array_rows "id UInt32, al UInt32 ALIAS id + 1, q Array(UInt32) DEFAULT arrayMap(x -> x + al, [id])" "" "*"
check lambda_parameter array_rows "id UInt32, al UInt32 ALIAS id + 1, q Array(UInt32) DEFAULT arrayMap(al -> al + 1, [id])" "" "*"

# Lambda parameters that would capture a column read by the inlined value.
check lambda_capture array_rows "id UInt32, al UInt32 ALIAS id + 1, q Array(UInt32) DEFAULT arrayMap(id -> id + al, [7])" "" "*"
check lambda_capture_member array_rows "id UInt32, al UInt32 ALIAS id + 1,
    q Array(UInt32) DEFAULT arrayMap(id -> id.x + al, [CAST(tuple(7), 'Tuple(x UInt32)')])" "" "*"

# An identifier named like an expression alias reads the alias, not the unwritten column `a`, and an alias of a column that
# no `DEFAULT` reads changes nothing.
check expression_alias rows "id UInt32, a UInt32, q UInt32 DEFAULT (toUInt32(5) AS a) + a" "(id, q)" "id, q"
check expression_alias_in_lambda rows "id UInt32, a UInt32, q UInt32 DEFAULT (toUInt32(5) AS a) + arrayMap(x -> x + a, [id])[1]" \
    "(id, q)" "id, q"
check expression_alias_of_unread_column rows "id UInt32, a UInt32, v UInt32 ALIAS (toUInt32(5) AS a) + 1, q UInt32 DEFAULT a + 1" \
    "(id, q)" "id, q"

# Columns left out of the column list: one with a `DEFAULT`, one without.
check column_list column_list_rows "id UInt32, od UInt32 DEFAULT id * 10, nd UInt32, q_od UInt32 DEFAULT od + 1, q_nd UInt32 DEFAULT nd + 1" \
    "(id, q_od, q_nd)" "id, q_od, q_nd"

# The input field `id` is inserted into `x`, so the `DEFAULT` of `q` reads the unwritten column `id`, not the field.
check collision rows "x UInt32, id UInt32 DEFAULT 5, q UInt32 DEFAULT id + 1" "(x, q)" "id, q" '{"x":1}
{"x":2,"q":100}'

check dict_get rows "id UInt32, name String ALIAS '${CLICKHOUSE_DATABASE}.kv_dictionary', q UInt32 DEFAULT dictGet(name, 'v', id)" "" "*"

check diamond rows "id UInt32, a1 UInt32 ALIAS id + id, a2 UInt32 ALIAS a1 + a1, a3 UInt32 ALIAS a2 + a2, a4 UInt32 ALIAS a3 + a3,
    a5 UInt32 ALIAS a4 + a4, q UInt32 DEFAULT a5 + 1" "" "*"

$CLICKHOUSE_CLIENT -q "${SETUP}"

for name in "${NAMES[@]}"; do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- \
        <<< "INSERT INTO ${name}_direct ${INSERT_COLUMNS[$name]} FORMAT JSONEachRow ${DIRECT_ROWS[$name]}"
    # `input` is fed by the client, which computes the `DEFAULT`s itself and has no dictionaries.
    if [[ $name != dict_get ]]; then
        $CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_input ${INSERT_COLUMNS[$name]} SELECT ${SELECT_LIST[$name]} FROM input() FORMAT JSONEachRow" \
            <<< "${INPUT[${INPUT_OF[$name]}]}"
    fi
done

$CLICKHOUSE_CLIENT --ignore-error -q "${FILE_INSERTS}"
$CLICKHOUSE_CLIENT --ignore-error -q "${S3CLUSTER_INSERTS}"
$CLICKHOUSE_CLIENT -q "${PRINT}"

# The expansion of the unwritten columns is bounded by `max_expanded_ast_elements`.
for function in "file('${FILE_DIR}/rows.jsonl', 'JSONEachRow')" "s3Cluster('${CLUSTER}', '${S3_DIR}/rows.jsonl', 'JSONEachRow')"; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO diamond_file SELECT * FROM ${function}
        SETTINGS max_expanded_ast_elements = 100, parallel_distributed_insert_select = 0" 2>&1 | grep -o -m1 'TOO_BIG_AST'
done

# The table computes `m` itself, and the table function cannot compute the same non-deterministic value for `q`, so the
# read of `m` is not substituted.
$CLICKHOUSE_CLIENT -q "CREATE TABLE volatile_default (id UInt32, m UInt32 MATERIALIZED rand(), q UInt32 DEFAULT m) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO volatile_default SELECT * FROM file('${FILE_DIR}/rows.jsonl', 'JSONEachRow')" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

# Nothing is substituted when the `DEFAULT`s or the values they read define an expression alias, which any of the `DEFAULT`s
# evaluated together may read: `q` reads the alias `a` of `r`, and `al` is not substituted.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE alias_of_other_default (id UInt32, a UInt32, q UInt32 DEFAULT a + 1, r UInt32 DEFAULT (toUInt32(7) AS a) * 2)
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO alias_of_other_default (id, q, r) SELECT id, q, r FROM file('${FILE_DIR}/rows.jsonl', 'JSONEachRow');
    SELECT 'alias_of_other_default', * FROM alias_of_other_default ORDER BY ALL;
    CREATE TABLE alias_in_value (id UInt32, x UInt32, al UInt32 ALIAS (id AS x) * 2, q UInt32 DEFAULT arrayMap(y -> al, [7])[1] + x)
    ENGINE = MergeTree ORDER BY id;"
$CLICKHOUSE_CLIENT -q "INSERT INTO alias_in_value SELECT * FROM file('${FILE_DIR}/x_rows.jsonl', 'JSONEachRow')" 2>&1 \
    | grep -o -m1 'UNKNOWN_IDENTIFIER'

rm -rf "${USER_FILES_PATH:?}/${FILE_DIR}"
