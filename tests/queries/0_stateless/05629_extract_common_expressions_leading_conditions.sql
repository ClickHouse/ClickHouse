-- The conditions that every disjunct starts with keep their place in front of the rest of the OR.

-- Compact parts have no per-column sizes and the table has no statistics, so PREWHERE keeps the written order.
DROP TABLE IF EXISTS t_extract_common_leading;
CREATE TABLE t_extract_common_leading (k LowCardinality(String), s String, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, auto_statistics_types = '';
INSERT INTO t_extract_common_leading SELECT 'a', toString(number), map('k1', if(number % 2 = 0, 'true', 'false')) FROM numbers(100);
SELECT position(explain, 'positionCaseInsensitiveUTF8') < position(explain, 'k1')
FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM t_extract_common_leading
    WHERE k = 'a' AND ((positionCaseInsensitiveUTF8(s, '1') AND m['k1'] = 'true') OR (positionCaseInsensitiveUTF8(s, '1') AND m['k1'] = 'false'))
    SETTINGS optimize_extract_common_expressions = 1, optimize_move_to_prewhere = 1, allow_reorder_prewhere_conditions = 1,
        move_all_conditions_to_prewhere = 1, use_statistics = 0
)
WHERE explain LIKE '%Prewhere filter column%';
DROP TABLE t_extract_common_leading;

-- A leading condition guards the rest of every disjunct, a trailing one is guarded by it.
SET optimize_extract_common_expressions = 1, short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0;
SELECT count() FROM (SELECT number % 10 AS x, toString(number) AS s FROM numbers(10))
WHERE s != 'zz' AND ((x != 0 AND intDiv(10, x) = 1) OR (x != 0 AND intDiv(10, x) = 2));
SELECT count() FROM (SELECT number % 10 AS x, toString(number) AS s FROM numbers(10))
WHERE s != 'zz' AND ((x != 0 AND intDiv(10, x) > 0) OR (x > 5 AND intDiv(10, x) > 0));
