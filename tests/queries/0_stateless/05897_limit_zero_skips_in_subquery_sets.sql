-- A query whose own LIMIT takes no row does not build the sets of its IN subqueries.
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS in_projection FROM numbers(10) LIMIT 0 FORMAT TSVWithNames;
SELECT number AS where_order_by FROM numbers(10) WHERE number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) ORDER BY number LIMIT 0 FORMAT TSVWithNames;
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS limit_offset FROM numbers(10) LIMIT 0 OFFSET 5 FORMAT TSVWithNames;
SELECT n IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS limit_in_subquery FROM (SELECT number AS n FROM numbers(10) LIMIT 0) FORMAT TSVWithNames;
SELECT n IN (SELECT throwIf(number = 0, 'outer set built') FROM numbers(1)) AS nested_set FROM (SELECT number AS n FROM numbers(10) WHERE number IN (SELECT throwIf(number = 0, 'inner set built') FROM numbers(1))) LIMIT 0 FORMAT TSVWithNames;
SELECT number AS result_filter FROM numbers(10) LIMIT 0 SETTINGS additional_result_filter = 'result_filter IN (SELECT throwIf(number = 0, ''set built'') FROM numbers(1))' FORMAT TSVWithNames;
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS with_extremes FROM numbers(10) LIMIT 0 SETTINGS extremes = 1 FORMAT TSVWithNames;
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS on_shards FROM remote('127.0.0.{1,2}', numbers(10)) LIMIT 0 SETTINGS enable_parallel_blocks_marshalling = 1 FORMAT TSVWithNames;
-- The set is needed: rows are read, or the totals row uses it.
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) FROM numbers(10) LIMIT 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT number IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) FROM numbers(10) LIMIT 0 SETTINGS exact_rows_before_limit = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT number % 2 IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) AS k, count() FROM numbers(10) GROUP BY k WITH TOTALS LIMIT 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT n IN (SELECT throwIf(number = 0, 'set built') FROM numbers(1)) FROM (SELECT number AS n FROM numbers(4) GROUP BY n WITH TOTALS) LIMIT 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- A materialized CTE read in the cut-off branch is still materialized for its other reader.
SELECT count(), sum(x) FROM (WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10)) SELECT x FROM (SELECT id IN (SELECT id FROM a) AS x FROM a LIMIT 0 UNION ALL SELECT id = 1 AS x FROM a)) SETTINGS enable_materialized_cte = 1;
