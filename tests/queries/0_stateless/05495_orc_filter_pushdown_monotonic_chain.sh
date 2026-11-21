#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cp "$CUR_DIR/data_orc/struct_null_field_statistics.orc" "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_05495_struct_nulls.orc"

$CLICKHOUSE_CLIENT --multiquery <<'EOF'
-- ORC filter pushdown through a monotonic function chain over a column.
-- `count()` under `indexHint` is the number of rows read, each row group has 10000 rows.

SET output_format_orc_row_index_stride = 10000;
SET input_format_orc_filter_push_down = 1;
SET engine_file_truncate_on_insert = 1;
SET max_insert_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05495.orc', ORC) SELECT number AS id FROM numbers(1000000);

SELECT 'rows', toTypeName(id), count() FROM file(currentDatabase() || '_05495.orc', ORC) GROUP BY toTypeName(id);

SELECT 'bare', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id = 555555);
SELECT 'between', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id BETWEEN 555555 AND 555555);
SELECT 'toUInt64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toUInt64(id) = 555555);
SELECT 'toInt64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toInt64(id) = 555555);
SELECT 'toFloat64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toFloat64(id) = 555555.0);
SELECT 'plus0', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 0 = 555555);
SELECT 'minus0', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id - 0 = 555555);
SELECT 'mul1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id * 1 = 555555);
SELECT 'plus1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 = 555556);
SELECT 'mul2', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id * 2 = 1111110);
SELECT 'intDiv1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(intDiv(id, 1) = 555555);
SELECT 'intDiv10', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(intDiv(id, 10) = 55555);
SELECT 'neg', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(-id = -555555);
SELECT 'in', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 IN (1, 500001, 999999));
SELECT 'mixed', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 = 555556 OR id = 5);

-- The same results with and without the pushdown.
SELECT 'intDiv10', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE intDiv(id, 10) = 55555;
SELECT 'intDiv10', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE intDiv(id, 10) = 55555
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'neg', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE -id = -555555;
SELECT 'neg', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE -id = -555555
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'in 8192', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 8192;
SELECT 'in 8192', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 8192, input_format_orc_filter_push_down = 0;
SELECT 'in 10000', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 10000;
SELECT 'in 10000', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 10000, input_format_orc_filter_push_down = 0;

-- A NULL satisfies `IS NULL` in a row group whose other values do not satisfy the chain.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_null.orc', ORC)
    SELECT if(number = 5000, NULL, 1000000 + number)::Nullable(Int64) AS id FROM numbers(200000);
SELECT 'null or', count() FROM file(currentDatabase() || '_05495_null.orc', ORC) WHERE id + 1 = 999 OR id IS NULL;
SELECT 'null kept', count() FROM file(currentDatabase() || '_05495_null.orc', ORC) WHERE indexHint(intDiv(id, 10) = 110000);

-- A chain or a set can match a NULL, so a row group with a NULL is not pruned.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_ifnull.orc', ORC)
    SELECT if(number = 0, NULL, if(number < 10000, -1, number))::Nullable(Int64) AS id FROM numbers(20000);
SELECT 'ifNull', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE ifNull(id, toInt64(42)) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0;
SELECT 'ifNull', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE ifNull(id, toInt64(42)) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0, input_format_orc_filter_push_down = 0;
SELECT 'coalesce', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE coalesce(id, toInt64(42)) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0;
SELECT 'negate ifNull', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE ifNull(-id, toInt64(42)) = 42
    SETTINGS allow_key_condition_coalesce_rewrite = 0;
SELECT 'toFloat64 ifNull', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE toFloat64(ifNull(id, toInt64(42))) = 42;
SELECT 'ifNull in', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE ifNull(id, toInt64(42)) IN (42, 43);
SELECT 'null in', count() FROM file(currentDatabase() || '_05495_ifnull.orc', ORC) WHERE intDiv(id, 10) IN (NULL, 5)
    SETTINGS transform_null_in = 1;

-- With `input_format_null_as_default` a NULL is read as the default of a non-Nullable column.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_default.orc', ORC)
    SELECT if(number % 7 = 0, NULL, number + 1)::Nullable(Int64) AS id FROM numbers(200000);
SELECT 'default', count() FROM file(currentDatabase() || '_05495_default.orc', ORC, 'id Int64') WHERE id + 1 = 1
    SETTINGS input_format_null_as_default = 1;
SELECT 'default pruned', count() FROM file(currentDatabase() || '_05495_default.orc', ORC, 'id Int64') WHERE indexHint(id + 1 = 190002) SETTINGS input_format_null_as_default = 1;

-- A tuple element prunes like a column. `indexHint` does not keep the subcolumn rewrite, so the rows read come from `query_log`.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_tuple.orc', ORC) SELECT tuple(number)::Tuple(x Int64) AS t FROM numbers(200000);
SELECT 'tuple', count(), sum(t.x) FROM file(currentDatabase() || '_05495_tuple.orc', ORC) WHERE t.x + 1 = 100001
    SETTINGS optimize_functions_to_subcolumns = 1, log_comment = '05495_tuple';
SELECT 'tuple', count(), sum(t.x) FROM file(currentDatabase() || '_05495_tuple.orc', ORC) WHERE t.x + 1 = 100001
    SETTINGS optimize_functions_to_subcolumns = 1, input_format_orc_filter_push_down = 0;

-- `t.x.y` is the element named `x.y`, which the file stores after the field `y` of the element `x`.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_dotted.orc', ORC)
    SELECT if(number < 10000, tuple(tuple(toInt64(100)), toInt64(7)), tuple(tuple(toInt64(7)), toInt64(100)))::Tuple(x Tuple(y Int64), `x.y` Int64) AS t
    FROM numbers(20000);
SELECT 'dotted', count() FROM file(currentDatabase() || '_05495_dotted.orc', ORC, 't Tuple(`x.y` Int64, x Tuple(y Int64))')
    WHERE t.x.y + 1 = 8 SETTINGS optimize_functions_to_subcolumns = 1, log_comment = '05495_dotted';
SELECT 'dotted', count() FROM file(currentDatabase() || '_05495_dotted.orc', ORC, 't Tuple(`x.y` Int64, x Tuple(y Int64))')
    WHERE t.x.y = 7 SETTINGS optimize_functions_to_subcolumns = 1;

SYSTEM FLUSH LOGS query_log;
SELECT 'tuple', read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '05495_tuple' AND type = 'QueryFinish';
SELECT 'dotted', read_rows FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '05495_dotted' AND type = 'QueryFinish';

-- A field has no value where its struct is NULL; this file does not count those rows in the field's statistics.
SELECT 'struct nulls', count() FROM file(currentDatabase() || '_05495_struct_nulls.orc', ORC, 't Nullable(Tuple(x Nullable(Int64)))')
    WHERE t.x + 1 = 5 OR t.x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'struct nulls flat', count() FROM file(currentDatabase() || '_05495_struct_nulls.orc', ORC, '`t.x` Int64')
    WHERE t.x + 1 = 1 SETTINGS input_format_null_as_default = 1;
EOF

rm -f "$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_05495_struct_nulls.orc"
