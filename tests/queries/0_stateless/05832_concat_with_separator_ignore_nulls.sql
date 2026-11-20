SELECT concatWithSeparatorIgnoreNulls(',', 'x', NULL, 'y') = 'x,y';
SELECT concatWithSeparatorIgnoreNulls(',', NULL, 'x', NULL, NULL, 'y', NULL) = 'x,y';
SELECT concatWithSeparatorIgnoreNulls(',', NULL, 'x', NULL) = 'x';
SELECT concatWithSeparatorIgnoreNulls(',', '', NULL, 'y') = ',y';
SELECT concatWithSeparatorIgnoreNulls(',', 'x', NULL, '', NULL, 'y') = 'x,,y';
SELECT concatWithSeparatorIgnoreNulls(',', NULL, NULL) = '';
SELECT concatWithSeparatorIgnoreNulls(',') = '';
SELECT concatWithSeparatorIgnoreNulls('', 'x', NULL, 'y') = 'xy';
WITH concatWithSeparatorIgnoreNulls(NULL, 'x') AS r
SELECT isNull(r) AND toTypeName(r) = 'Nullable(String)';
WITH concatWithSeparatorIgnoreNulls(NULL) AS r
SELECT isNull(r) AND toTypeName(r) = 'Nullable(String)';
WITH concatWithSeparatorIgnoreNulls(CAST(NULL AS Nullable(String))) AS r
SELECT isNull(r) AND toTypeName(r) = 'Nullable(String)';
WITH concatWithSeparatorIgnoreNulls(toNullable(',')) AS r
SELECT r = '' AND toTypeName(r) = 'Nullable(String)';

SELECT a FROM values('a Int32, b Nullable(Int32)', (2, NULL), (3, 4))
WHERE concatWithSeparatorIgnoreNulls('/', a, b) = '2';
SELECT a FROM values('a Int32, b Int32, expected String', (2, 3, '2'), (3, 4, '3/4'))
WHERE concatWithSeparatorIgnoreNulls('/', a, nullIf(b, 3)) = expected ORDER BY a;
SELECT count() = 0 AND toTypeName(groupArray(tuple(r))) = 'Array(Tuple(String))'
FROM (SELECT concatWithSeparatorIgnoreNulls('/', number) AS r FROM numbers(0));
SELECT count() = 0 AND toTypeName(groupArray(tuple(r))) = 'Array(Tuple(Nullable(String)))'
FROM (SELECT concatWithSeparatorIgnoreNulls(toNullable('/'), number) AS r FROM numbers(0));

SELECT concatWithSeparatorIgnoreNulls(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT concatWithSeparatorIgnoreNulls(1, 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT concatWithSeparatorIgnoreNulls(CAST(NULL AS Nullable(UInt8)), 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT concatWithSeparatorIgnoreNulls(1, NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT concatWithSeparatorIgnoreNulls(1, number) FROM numbers(0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT concatWithSeparatorIgnoreNulls(materialize(',')); -- { serverError ILLEGAL_COLUMN }
SELECT concatWithSeparatorIgnoreNulls(materialize(','), 'x'); -- { serverError ILLEGAL_COLUMN }
SELECT concatWithSeparatorIgnoreNulls(materialize(CAST(NULL AS LowCardinality(Nullable(FixedString(2)))))); -- { serverError ILLEGAL_COLUMN }
SELECT concatWithSeparatorIgnoreNulls(toString(number)) FROM numbers(0); -- { serverError ILLEGAL_COLUMN }
SELECT concatWithSeparatorIgnoreNulls(toString(number), number) FROM numbers(0); -- { serverError ILLEGAL_COLUMN }
SELECT CONCATWITHSEPARATORIGNORENULLS(',', 'x'); -- { serverError UNKNOWN_FUNCTION }
SELECT isNull(concatWithSeparator(',', 'x', NULL, 'y'))
   AND isNull(concat_ws(',', 'x', NULL, 'y'))
   AND isNull(concatWithSeparatorAssumeInjective(',', 'x', NULL, 'y'));

SELECT concatWithSeparatorIgnoreNulls(',', a, b) = expected
FROM values('n UInt8, a Nullable(String), b Nullable(String), expected String',
    (0, 'x', NULL, 'x'), (1, NULL, NULL, ''), (2, '', 'y', ',y')) ORDER BY n;
SELECT isNull(concatWithSeparatorIgnoreNulls(CAST(NULL AS Nullable(String)), a, b))
FROM values('a Int32, b Nullable(Int32)', (2, NULL), (3, 4)) ORDER BY a;
SELECT concatWithSeparatorIgnoreNulls(sep, 'x', NULL, 'y') = 'x,y' FROM (SELECT ',' AS sep);
SELECT concatWithSeparatorIgnoreNulls((SELECT '|'), 'x', NULL, 'y') = 'x|y';
WITH concatWithSeparatorIgnoreNulls(toNullable(','), 'x', NULL, 'y') AS r
SELECT r = 'x,y' AND toTypeName(r) = 'Nullable(String)';
WITH
    CAST(',' AS LowCardinality(Nullable(String))) AS sep,
    CAST(if(number = 0, NULL, 'x') AS LowCardinality(Nullable(String))) AS v,
    concatWithSeparatorIgnoreNulls(sep, v, 'y') AS r,
    concatWithSeparatorIgnoreNulls(',', v, 'y') AS s
SELECT r = if(number = 0, 'y', 'x,y') AND toTypeName(r) = 'Nullable(String)'
   AND s = r AND toTypeName(s) = 'String'
FROM numbers(2) ORDER BY number;
WITH
    concatWithSeparatorIgnoreNulls(CAST(',' AS LowCardinality(String)), 'x', NULL, 'y') AS ordinary_separator_result,
    concatWithSeparatorIgnoreNulls(CAST(NULL AS LowCardinality(Nullable(FixedString(2)))), 'x') AS null_separator_result
SELECT ordinary_separator_result = 'x,y' AND toTypeName(ordinary_separator_result) = 'String'
   AND isNull(null_separator_result) AND toTypeName(null_separator_result) = 'Nullable(String)';
WITH
    if(number = 0, NULL, 'x') AS v,
    concatWithSeparatorIgnoreNulls(',', CAST(v AS Nullable(Enum8('x' = 1))), 'y') AS e,
    concatWithSeparatorIgnoreNulls(',', CAST(v AS Dynamic), 'y') AS d,
    concatWithSeparatorIgnoreNulls(',', CAST(v AS Variant(String, UInt64)), 'y') AS w
SELECT e = if(number = 0, 'y', 'x,y') AND e = d AND d = w
   AND toTypeName(e) = 'String' AND toTypeName(d) = 'String' AND toTypeName(w) = 'String'
FROM numbers(2) ORDER BY number;
WITH concatWithSeparatorIgnoreNulls(',', CAST(NULL AS Dynamic), CAST(toUInt64(5) AS Dynamic),
    CAST(toUInt64(42) AS Variant(String, UInt64))) AS r
SELECT r = '5,42' AND toTypeName(r) = 'String';

SELECT concatWithSeparatorIgnoreNulls('|', 'x', 42, materialize([1, NULL]), tuple(2, NULL), map('k', NULL))
    = 'x|42|[1,NULL]|(2,NULL)|{\'k\':NULL}';
SELECT concatWithSeparatorIgnoreNulls(',', a, b, 'foo', c) = expected
FROM values('a Int32, b Int32, c Nullable(DateTime(\'UTC\')), expected String',
    (2, 3, '2019-01-02 10:00:00', '2,3,foo,2019-01-02 10:00:00'), (3, 4, NULL, '3,4,foo')) ORDER BY a;
SELECT hex(concatWithSeparatorIgnoreNulls(toFixedString('|', 2), toFixedString('x', 2), NULL, toFixedString('y', 2))) = '78007C007900';
WITH concatWithSeparatorIgnoreNulls('|', v, 'y') AS r
SELECT hex(r) = expected AND toTypeName(r) = 'String'
FROM values('n UInt8, v Nullable(FixedString(2)), expected String', (0, NULL, '79'), (1, 'x', '78007C79'))
ORDER BY n;
WITH concatWithSeparatorIgnoreNulls(toNullable(toFixedString('|', 2)), toString(number), 'y') AS r
SELECT hex(r) = concat(hex(toString(number)), '7C0079') AND toTypeName(r) = 'Nullable(String)'
FROM numbers(2) ORDER BY number;
SELECT hex(concatWithSeparatorIgnoreNulls(',', materialize(unhex('780079')), NULL, 'z')) = '7800792C7A';
SELECT concatWithSeparatorIgnoreNulls(concatWithSeparatorIgnoreNulls('', NULL, '/', '-'), 'x', 'y') = 'x/-y';

-- Validate the separator while inferring the header, even for an unselected branch.
SELECT if(number = 0, 'ok', concatWithSeparatorIgnoreNulls(toString(number), 'x'))
FROM numbers(1) SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_COLUMN }
SELECT multiIf(number = 0, 'ok', number = 1, 'other', concatWithSeparatorIgnoreNulls(toString(number), 'x'))
FROM numbers(1) SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_COLUMN }

SELECT if(number = 0, 'ok', concatWithSeparatorIgnoreNulls(toString(number), 'x'))
FROM numbers(2) SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_COLUMN }
SELECT multiIf(number = 0, 'ok', number = 1, 'other', concatWithSeparatorIgnoreNulls(toString(number), 'x'))
FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_COLUMN }
