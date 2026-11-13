-- A lazily executed expression with several uses is executed as many times as without short-circuit evaluation.

DROP DICTIONARY IF EXISTS d_05628;
DROP TABLE IF EXISTS t_05628;

CREATE TABLE t_05628 (k String, a1 Array(UInt64), a2 Array(UInt64), a3 Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05628 SELECT toString(number), range(number % 10), arrayMap(i -> i * 2, range(number % 10)), arrayMap(i -> toUInt8(i % 2), range(number % 10)) FROM numbers(100);
CREATE DICTIONARY d_05628 (k String, a1 Array(UInt64), a2 Array(UInt64), a3 Array(UInt8))
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 't_05628')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());
SELECT dictGet('d_05628', 'a1', '0') FORMAT Null;

SET compile_expressions = 0, max_threads = 1, max_block_size = 65536;

SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND length(repeat(toString(number), 10)) + position(repeat(toString(number), 10), '7') + countSubstrings(repeat(toString(number), 10), '1') > 25
SETTINGS short_circuit_function_evaluation = 'enable', log_comment = '05628_plus_enable';
SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND length(repeat(toString(number), 10)) + position(repeat(toString(number), 10), '7') + countSubstrings(repeat(toString(number), 10), '1') > 25
SETTINGS short_circuit_function_evaluation = 'disable', log_comment = '05628_plus_disable';

SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND multiIf(length(repeat(toString(number), 10)) > 25, 1, position(repeat(toString(number), 10), '7') > 0, 2, 3) = 2
SETTINGS short_circuit_function_evaluation = 'enable', log_comment = '05628_multi_if_enable';
SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND multiIf(length(repeat(toString(number), 10)) > 25, 1, position(repeat(toString(number), 10), '7') > 0, 2, 3) = 2
SETTINGS short_circuit_function_evaluation = 'disable', log_comment = '05628_multi_if_disable';

SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND (length(repeat(toString(number), 10)) > 25 OR position(repeat(toString(number), 10), '7') > 0)
SETTINGS short_circuit_function_evaluation = 'enable', log_comment = '05628_or_enable';
SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND (length(repeat(toString(number), 10)) > 25 OR position(repeat(toString(number), 10), '7') > 0)
SETTINGS short_circuit_function_evaluation = 'disable', log_comment = '05628_or_disable';

SELECT count() FROM numbers(1000) WHERE number % 10 != 0
    AND ((cityHash64(repeat(toString(number), 10)) % 4 = 1 AND number % 3 != 0) OR (cityHash64(repeat(toString(number), 10)) % 4 IN (2, 3) AND number % 5 != 0))
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0, log_comment = '05628_or_of_and_enable';
SELECT count() FROM numbers(1000) WHERE number % 10 != 0
    AND ((cityHash64(repeat(toString(number), 10)) % 4 = 1 AND number % 3 != 0) OR (cityHash64(repeat(toString(number), 10)) % 4 IN (2, 3) AND number % 5 != 0))
SETTINGS short_circuit_function_evaluation = 'disable', short_circuit_function_evaluation_reorder_arguments = 0, log_comment = '05628_or_of_and_disable';

SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND notEmpty(arrayFilter(t -> t.3 = 0, arrayZip(untuple(dictGet('d_05628', ('a1', 'a2', 'a3'), toString(number % 100))))))
SETTINGS short_circuit_function_evaluation = 'enable', optimize_dictget_tuple_element = 0, log_comment = '05628_dict_get_enable';
SELECT count() FROM numbers(1000) WHERE number % 2 = 0
    AND notEmpty(arrayFilter(t -> t.3 = 0, arrayZip(untuple(dictGet('d_05628', ('a1', 'a2', 'a3'), toString(number % 100))))))
SETTINGS short_circuit_function_evaluation = 'disable', optimize_dictget_tuple_element = 0, log_comment = '05628_dict_get_disable';

-- The reused results were computed on the rows of the outer argument anyway, so they cannot throw on other rows.
SELECT count() FROM numbers(1000) WHERE number % 7 != 0
    AND multiIf(intDiv(7000, number % 7) > 2000, 1, intDiv(7000, number % 7) > 1000, 2, 3) = 2
SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count() FROM numbers(1000) WHERE number % 7 != 0 AND intDiv(7000, number % 7) > 1000 AND intDiv(7000, number % 7) < 3000
SETTINGS short_circuit_function_evaluation = 'enable';

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceRegexpOne(log_comment, '_(enable|disable)$', '') AS name,
    maxIf(ProfileEvents['FunctionExecute'], log_comment LIKE '%\_enable') = maxIf(ProfileEvents['FunctionExecute'], log_comment LIKE '%\_disable')
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^05628_.*_(enable|disable)$')
GROUP BY name
ORDER BY name;

DROP DICTIONARY d_05628;
DROP TABLE t_05628;
