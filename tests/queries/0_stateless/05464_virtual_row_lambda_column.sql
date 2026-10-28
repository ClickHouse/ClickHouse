-- With `read_in_order_use_virtual_row`, the first chunk of an in-order read was built with `createColumn` for each column of
-- the header, which a `Function` column of a lambda does not support, so the query failed with `NOT_IMPLEMENTED`:
-- https://github.com/ClickHouse/ClickHouse/issues/127748

DROP TABLE IF EXISTS t_virtual_row_lambda;
CREATE TABLE t_virtual_row_lambda (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_virtual_row_lambda SELECT number, ['a', 'b', 'c'][number % 3 + 1] FROM numbers(20);

SET optimize_read_in_order = 1, optimize_distinct_in_order = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT DISTINCT k, arrayFirst(x -> x = s, ['a', 'b']) AS f FROM t_virtual_row_lambda
WHERE arrayExists(x -> x = s, ['a', 'b']) ORDER BY k LIMIT 5 SETTINGS read_in_order_use_virtual_row = 1;

DROP TABLE t_virtual_row_lambda;
