-- A `String` column with dictionary granules gives the same results as its `with_size_stream` twin in an `IN`
-- subquery, whose set takes each used value once (and every row for a tuple of two columns) and keeps its
-- elements for the analysis of the primary key, which starts with the column, in the arguments that
-- short-circuit evaluation executes lazily, and through the lazy materialization of `ORDER BY ... LIMIT`,
-- whose squashing compacts it. A function that can throw is not executed on a value whose rows the condition
-- removes.

DROP TABLE IF EXISTS t_lazy_plain;
DROP TABLE IF EXISTS t_lazy_dict;

CREATE TABLE t_lazy_plain (id UInt64, s String, s2 String)
ENGINE = MergeTree ORDER BY (s, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_lazy_dict (id UInt64, s String, s2 String)
ENGINE = MergeTree ORDER BY (s, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

-- A value of `s` is a number after `n` or a word after `x`: `toUInt64` throws on the words.
INSERT INTO t_lazy_plain SELECT number, if(number % 3 = 0, 'n' || toString(number % 40), 'x' || toString(number % 7) || 'word'), 'w' || toString(number % 11) FROM numbers(10000);
INSERT INTO t_lazy_dict SELECT number, if(number % 3 = 0, 'n' || toString(number % 40), 'x' || toString(number % 7) || 'word'), 'w' || toString(number % 11) FROM numbers(10000);

SELECT 'in', (SELECT count() FROM t_lazy_plain WHERE s IN (SELECT s FROM t_lazy_plain WHERE id % 7 = 0))
    = (SELECT count() FROM t_lazy_dict WHERE s IN (SELECT s FROM t_lazy_dict WHERE id % 7 = 0));
SELECT 'in, tuple', (SELECT count() FROM t_lazy_plain WHERE (s, s2) IN (SELECT s, s2 FROM t_lazy_plain WHERE id % 7 = 0))
    = (SELECT count() FROM t_lazy_dict WHERE (s, s2) IN (SELECT s, s2 FROM t_lazy_dict WHERE id % 7 = 0));

SET short_circuit_function_evaluation = 'force_enable';
SELECT 'short circuit', (SELECT count() FROM t_lazy_plain WHERE id % 2 = 0 OR s LIKE '%3%' OR s2 LIKE '%1')
    = (SELECT count() FROM t_lazy_dict WHERE id % 2 = 0 OR s LIKE '%3%' OR s2 LIKE '%1');
SELECT 'throwing function under a condition', (SELECT sum(if(startsWith(s, 'n'), toUInt64(substring(s, 2)), 0)) FROM t_lazy_plain)
    = (SELECT sum(if(startsWith(s, 'n'), toUInt64(substring(s, 2)), 0)) FROM t_lazy_dict);
SELECT 'lower', (SELECT sum(cityHash64(lower(s))) FROM t_lazy_plain) = (SELECT sum(cityHash64(lower(s))) FROM t_lazy_dict);

SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100;
SELECT 'lazy materialization', (SELECT groupArray(tuple(*)) FROM (SELECT * FROM t_lazy_plain ORDER BY id DESC LIMIT 50))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM t_lazy_dict ORDER BY id DESC LIMIT 50));

DROP TABLE t_lazy_plain;
DROP TABLE t_lazy_dict;
