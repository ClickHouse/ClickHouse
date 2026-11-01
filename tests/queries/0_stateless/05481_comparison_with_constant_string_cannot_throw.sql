-- A comparison of a column with a constant string that converts to the type of the column compares values of that
-- type, so it cannot throw. Short-circuit evaluation can then evaluate it before an argument that throws on the rows
-- it rejects, and a PREWHERE step does not have to filter the block before it.

SET short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, query_plan_merge_filters = 1, optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS t_cmp_const_string;
CREATE TABLE t_cmp_const_string (x UInt64, d Date, dt DateTime('UTC'), n Int32, s String) ENGINE = Log;
-- x = 0 only in the rows of the year 2000.
INSERT INTO t_cmp_const_string SELECT number % 10, if(number % 10 = 0, toDate('2000-01-01'), toDate('2020-01-01')), if(number % 10 = 0, toDateTime('2000-01-01 00:00:00', 'UTC'), toDateTime('2020-01-01 00:00:00', 'UTC')), if(number % 10 = 0, -5, 5), if(number % 10 = 0, '', 'a') FROM numbers(100);

SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND d > '2010-01-01';
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND dt > '2010-01-01 00:00:00';
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND n > '0';

-- A string that does not convert keeps the comparison among those that can throw.
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND d > 'abc'; -- { serverError CANNOT_PARSE_DATE }
-- Without the reordering the division is evaluated first.
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND d > '2010-01-01' SETTINGS short_circuit_function_evaluation_reorder_arguments = 0; -- { serverError ILLEGAL_DIVISION }

-- `notEmpty` reads only the size of a string, and `s != ''` is rewritten to it (and not to `s.size != 0`).
SET optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND notEmpty(s);
SELECT count() FROM t_cmp_const_string WHERE intDiv(10, x) > 1 AND s != '';

DROP TABLE t_cmp_const_string;

-- The rows a PREWHERE step rejects may reach the next step only if it cannot throw on them. A comparison of a tuple of
-- a `Variant` is compared per row, even with a constant that converts.
SET enable_multiple_prewhere_read_steps = 1, enable_variant_type = 1, use_variant_default_implementation_for_comparisons = 1;

DROP TABLE IF EXISTS t_cmp_const_string_prewhere;
CREATE TABLE t_cmp_const_string_prewhere (k UInt64, g UInt8, tv Tuple(Variant(String, UInt64))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_cmp_const_string_prewhere SELECT number, number % 10 != 0, if(number % 10 = 0, tuple(1::UInt64)::Tuple(Variant(String, UInt64)), tuple('a')::Tuple(Variant(String, UInt64))) FROM numbers(100000);

SELECT count() FROM t_cmp_const_string_prewhere PREWHERE g = 1 AND tv = '(\'a\')';

DROP TABLE t_cmp_const_string_prewhere;
