-- A lazily executed expression with several uses is executed as many times as without short-circuit evaluation.

DROP DICTIONARY IF EXISTS d_05628;
DROP TABLE IF EXISTS t_05628;
DROP DICTIONARY IF EXISTS d2_05628;
DROP TABLE IF EXISTS t2_05628;

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

-- A reused result is still executed lazily by the function that needs it only on some rows, so its other rows are not converted.
WITH toDecimal32(number + 1000, 0) AS x
SELECT if(number > 0, multiIf(x > 0, toDecimal32(1, 9), number = 0, toDecimal32(0, 9), x), toDecimal32(0, 9))
FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable', optimize_multiif_to_if = 0;
WITH toDecimal32(number + 1000, 0) AS x
SELECT if(number > 0, multiIf(x > 1001, toDecimal32(1, 9), number = 7, toDecimal32(2, 9), multiIf(number = 100, x, number = 200, toDecimal32(2, 9), toDecimal32(0, 9))), toDecimal32(0, 9))
FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable', optimize_multiif_to_if = 0;

-- The reduced columns are replaced once inside a node shared by both branches of `if`.
WITH cityHash64(number) AS r, if(r % 3 = 0, r % 7, r % 11) AS a1, if(r % 2 = 0, a1 + 1, a1 * 2) AS a2, if(intDiv(r, 3) % 2 = 0, a2 + 1, a2 * 2) AS a3
SELECT sum(if(number < 1000000, multiIf(r % 5 = 0, 1, r % 5 = 1, a3, 0), 0)) FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'enable', optimize_multiif_to_if = 0, short_circuit_function_evaluation_reorder_arguments = 0;
WITH cityHash64(number) AS r, if(r % 3 = 0, r % 7, r % 11) AS a1, if(r % 2 = 0, a1 + 1, a1 * 2) AS a2, if(intDiv(r, 3) % 2 = 0, a2 + 1, a2 * 2) AS a3
SELECT sum(if(number < 1000000, multiIf(r % 5 = 0, 1, r % 5 = 1, a3, 0), 0)) FROM numbers(1000)
SETTINGS short_circuit_function_evaluation = 'disable';

CREATE TABLE t2_05628 (k UInt64, v UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t2_05628 SELECT number + 1000, number + 10 FROM numbers(3);
CREATE DICTIONARY d2_05628 (k UInt64, v UInt8) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 't2_05628')) LIFETIME(0) LAYOUT(HASHED());
SELECT if(number > 0, dictGetOrDefault('d2_05628', 'v', number + 1000, number + 1000), 0) FROM numbers(3)
SETTINGS short_circuit_function_evaluation = 'force_enable';

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
DROP DICTIONARY d2_05628;
DROP TABLE t2_05628;
