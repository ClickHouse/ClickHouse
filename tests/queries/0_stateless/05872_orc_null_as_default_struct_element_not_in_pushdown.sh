#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: ORC is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Filter push down keeps a NULL struct, read as the default of its non-Nullable element, for an atom with a built-in
# negation (`notIn`) that is true for the default. The file has `t struct<x:double>` with the rows (1), (1), NULL, (1),
# written by Spark 3.5.3: the Java writer writes the element only for non-NULL structs, so its statistics have no NULL.
for predicate in 'NOT (t.x > 0.5)' 'NOT (t.x IN (1, 2))' 't.x NOT IN (1, 2)' 'NOT (t.x NOT IN (1, 2))' 't.x IN (1, 2)'
do
    for push_down in 0 1
    do
        $CLICKHOUSE_LOCAL -q "
            SELECT '$predicate', $push_down, count()
            FROM file('$CUR_DIR/data_orc/spark_null_struct_element.orc', ORC, 't Tuple(x Float64)')
            WHERE $predicate
            SETTINGS input_format_orc_filter_push_down = $push_down, input_format_null_as_default = 1, transform_null_in = 0"
    done
done
