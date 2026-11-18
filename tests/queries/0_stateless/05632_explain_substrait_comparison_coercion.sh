#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Substrait is not built in fast test

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The type that each side of the comparison in WHERE is cast to. Empty for a side without a cast.
function cast_types()
{
    echo "
        SELECT
            JSON_QUERY(explain, '\$.relations[0].root.input.project.input.filter.condition.scalarFunction.arguments[0].value.cast.type'),
            JSON_QUERY(explain, '\$.relations[0].root.input.project.input.filter.condition.scalarFunction.arguments[1].value.cast.type')
        FROM (EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_coercion WHERE $1);"
}

# Of each exception only the message is printed.
$CLICKHOUSE_CLIENT --enable_substrait 1 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_coercion;
    CREATE TABLE t_substrait_coercion (id Int64, f32 Float32, f64 Float64, i32 Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_substrait_coercion (id) VALUES (1);

    SELECT 'The narrower float is cast to the wider one, whichever side it is on';
    $(cast_types 'f32 < f64')
    $(cast_types 'f64 < f32')

    SELECT 'An integer and a float have no common type, whichever side they are on';
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_coercion WHERE i32 < f64;
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_coercion WHERE f64 < i32;

    DROP TABLE t_substrait_coercion;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
