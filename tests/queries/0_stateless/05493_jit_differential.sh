#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# Differential test of the JIT: every function that is compiled, or that a planned change will compile, runs on edge
# values of each native type, with the JIT on and off. The outputs, including the errors, must be the same.
# The reference shows, per function, how many of its type combinations ran compiled, so that a change to what the JIT
# compiles shows up here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The types and their edge values. Each column takes its value from 8 edge slots; the `a` columns and the `b` columns use
# different selectors, so that every pair of edge values meets.
TYPES=(
    "Int8" "Int64" "Int128" "UInt8" "UInt64" "UInt128" "Float32" "Float64" "Decimal32(2)" "Decimal128(10)" "Date"
    "DateTime64(3, 'UTC')" "Enum8('a' = -128, 'b' = 0, 'c' = 1, 'd' = 127)" "Nullable(Int32)" "Nullable(Float64)")
EDGES=(
    "-128 127 0 -1 1 2 -2 100"
    "-9223372036854775808 9223372036854775807 0 -1 1 2 -2 4294967296"
    "-170141183460469231731687303715884105728 170141183460469231731687303715884105727 0 -1 1 2 -2 18446744073709551616"
    "0 255 1 2 128 127 3 10"
    "0 18446744073709551615 1 2 9223372036854775808 9223372036854775807 4294967295 10"
    "0 340282366920938463463374607431768211455 1 2 170141183460469231731687303715884105728 18446744073709551615 3 10"
    "nan inf -inf 0 -0 1 -1 3.4028235e38"
    "nan inf -inf 0 -0 1 -1 1.7976931348623157e308"
    "-9999999.99 9999999.99 0 -1 1 0.01 -0.01 2"
    "-9999999999999999999999999999.9999999999 9999999999999999999999999999.9999999999 0 -1 1 0.0000000001 2 -2"
    "1970-01-01 2149-06-06 2000-02-29 1970-01-02 2026-01-01 1999-12-31 2038-01-19 2106-02-07"
    "1900-01-01T00:00:00.000 2299-12-31T23:59:59.999 1970-01-01T00:00:00.000 1969-12-31T23:59:59.999 2026-01-01T00:00:00.001 2000-02-29T12:00:00.500 1970-01-01T00:00:00.001 2106-02-07T06:28:15.000"
    "a d b c a d b c"
    "NULL -2147483648 2147483647 0 -1 1 2 -2"
    "NULL nan inf -inf 0 -0 1 -1"
)

UNARY=(negate abs sign bitNot bitCount intExp2 sqr not isNull isNotNull assumeNotNull toNullable identity
    toUInt8 toUInt16 toUInt32 toUInt64 toUInt128 toInt8 toInt16 toInt32 toInt64 toInt128 toFloat32 toFloat64
    isZeroOrNull isNaN isFinite isInfinite)
BINARY=(plus minus multiply divide intDiv modulo positiveModulo intDivOrZero moduloOrZero intDivOrNull moduloOrNull
    divideOrNull bitAnd bitOr bitXor bitShiftLeft bitShiftRight bitRotateLeft bitRotateRight least greatest midpoint
    equals notEquals less greater lessOrEquals greaterOrEquals isDistinctFrom isNotDistinctFrom and or xor
    coalesce ifNull nullIf)

# The value of edge slot `selector` of type `t`, as an expression.
function edges_expression()
{
    local t=$1
    local selector=$2
    local expression="multiIf("
    local slot=0
    local values=($3)
    for value in "${values[@]}"; do
        local literal="CAST('${value//T/ }' AS $t)"
        if [ "$value" == "NULL" ]; then
            literal="NULL"
        fi
        # The last slot is the else branch.
        if [ $slot -lt $((${#values[@]} - 1)) ]; then
            expression+="$selector = $slot, $literal, "
        else
            expression+="$literal"
        fi
        slot=$((slot + 1))
    done
    echo "$expression) :: $t"
}

create="CREATE TABLE jit_differential (k UInt32, c1 UInt8, c2 Nullable(UInt8)"
fill="SELECT number, number % 3 = 0, if(number % 5 = 0, NULL, number % 2)"
for i in "${!TYPES[@]}"; do
    create+=", a$i ${TYPES[$i]}, b$i ${TYPES[$i]}"
    fill+=", $(edges_expression "${TYPES[$i]}" "number % 8" "${EDGES[$i]}")"
    fill+=", $(edges_expression "${TYPES[$i]}" "intDiv(number, 8) % 8" "${EDGES[$i]}")"
done
create+=") ENGINE = MergeTree ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_differential"
$CLICKHOUSE_CLIENT --query "$create"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_differential $fill FROM numbers(64)"

# One query per function and type combination. Each function is the argument of `identity`, which is compiled for every
# native type, so that the function is compiled whenever it can be (a function alone is never compiled).
# The label of each query is its first column.
queries="$CLICKHOUSE_TMP/jit_differential_queries.sql"
: > "$queries"
function add_query()
{
    local label=$1
    local expression=$2
    echo "SELECT '$label', cityHash64(groupArray(toString(identity($expression)))) FROM jit_differential;" >> "$queries"
}
for f in "${UNARY[@]}"; do
    for i in "${!TYPES[@]}"; do
        add_query "$f ${TYPES[$i]//\'/}" "$f(a$i)"
    done
done
for f in "${BINARY[@]}"; do
    for i in "${!TYPES[@]}"; do
        for j in "${!TYPES[@]}"; do
            add_query "$f ${TYPES[$i]//\'/}, ${TYPES[$j]//\'/}" "$f(a$i, b$j)"
        done
    done
done
for i in "${!TYPES[@]}"; do
    for j in "${!TYPES[@]}"; do
        add_query "if ${TYPES[$i]//\'/}, ${TYPES[$j]//\'/}" "if(c1, a$i, b$j)"
        add_query "if Nullable condition ${TYPES[$i]//\'/}, ${TYPES[$j]//\'/}" "if(c2, a$i, b$j)"
        add_query "multiIf ${TYPES[$i]//\'/}, ${TYPES[$j]//\'/}" "multiIf(c1, a$i, c2, b$j, a$i)"
        add_query "CAST ${TYPES[$i]//\'/}, ${TYPES[$j]//\'/}" "CAST(a$i AS ${TYPES[$j]})"
    done
done

# Prints the outputs of the queries, with errors reduced to their code and the start of their message, without the
# context the executor adds (which names the compiled function).
function run()
{
    # shellcheck disable=SC2068
    $CLICKHOUSE_CLIENT $@ --send_logs_level none --max_threads 1 --short_circuit_function_evaluation enable --short_circuit_function_evaluation_for_nulls_threshold 1 --ignore-error --queries-file "$queries" 2>&1 \
        | sed -E 's/^.*Code: ([0-9]+)\..*DB::Exception: ([^:(]*[^:( ]).*/Code \1: \2/'
}

compiled=$(run --compile_expressions 1 --min_count_to_compile_expression 0 --log_comment "jit_differential_$CLICKHOUSE_DATABASE")
interpreted=$(run --compile_expressions 0)

if [ "$compiled" == "$interpreted" ]; then
    echo "compiled and interpreted outputs are the same"
else
    echo "compiled and interpreted outputs differ:"
    diff <(echo "$interpreted") <(echo "$compiled") | head -50
fi

# How many type combinations of each function ran compiled, of those that ran without an error.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT
        extract(query, '^SELECT \'([A-Za-z0-9]+)') AS function,
        countIf(ProfileEvents['CompiledFunctionRows'] > 0) AS compiled,
        count() AS combinations
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment = 'jit_differential_$CLICKHOUSE_DATABASE' AND query LIKE 'SELECT \'%'
    GROUP BY function
    ORDER BY function
    FORMAT TSV"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_differential"
