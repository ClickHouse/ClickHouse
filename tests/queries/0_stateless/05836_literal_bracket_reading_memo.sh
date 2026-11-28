#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# The parser remembers brackets where reading a literal failed. Check that results and error messages stay the same.

$CLICKHOUSE_LOCAL -q "
SELECT formatQuerySingleLine('SELECT [1, 2]::Array(UInt8), (1, \'a\')::Tuple(UInt8, String)');
SELECT formatQuerySingleLine('SELECT [(1, 2), x], [[1, 2], [3, x]], (1, (2, (3, x))), ([1, 2], x), ((1)), (1, (2, 3))[1]');
SELECT formatQuerySingleLine('SELECT (((1)::UInt8)::UInt16)::UInt32, [[0x1], [2]]::Array(Array(UInt8)), [[1], [2]]::Array(Array(UInt8))');
SELECT formatQuerySingleLine('SELECT (1, (2, 3)) + x, [[1, 2], [3, 4]][1], ((1, 2), (3, x))::Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8))');
SELECT formatQuerySingleLine('SELECT [[1, [2, x]], [3]], [[1, 2], [3]], (1, (2, (3, 4)))::Tuple(UInt8, Tuple(UInt8, Tuple(UInt8, String)))');
SELECT (1, (2, (3, 4)))::Tuple(a UInt8, b Tuple(c UInt8, d Tuple(e UInt8, f UInt64))) AS t, toTypeName(t);
SELECT x, [(1, 2), (x, 3)], [[1, 2], [3, x]], (1, (2, (3, x))), ((1, 2), (3, x)), [[0x1], [2]]::Array(Array(UInt8)) FROM (SELECT 7 AS x);
SELECT [[1, 2], [3, 4]]::Array(Array(UInt16)) AS a, toTypeName(a);
SELECT cityHash64(formatQuery('SELECT ' || repeat('(1, ', 50) || 'x' || repeat(')', 50))) = cityHash64(formatQuery(formatQuery('SELECT ' || repeat('(1, ', 50) || 'x' || repeat(')', 50)))) SETTINGS max_parser_depth = 0;
SELECT length(formatQuery('SELECT ' || repeat('[1, ', 50) || 'x' || repeat(']', 50))) SETTINGS max_parser_depth = 0;
SELECT length(formatQuery('SELECT ' || repeat('((1, ', 50) || '1' || repeat('))', 50))) SETTINGS max_parser_depth = 0;
SELECT length(formatQuery('SELECT ' || repeat('(', 50) || '1' || repeat('::UInt8)', 50))) SETTINGS max_parser_depth = 0;
SELECT formatQuerySingleLine('SELECT [1, [1, [1, (2)]]], (1, (1, (1, (2)))), ((1, ((1, 2))))');
SELECT [[[(2)], [3]], [[4]]] AS a, toTypeName(a), (1, (1, (1, (2)))) AS b, toTypeName(b), ((1, ((1, 2)))) AS c, toTypeName(c);
SELECT length(formatQuery('SELECT ' || repeat('[1, ', 50) || '(2)' || repeat(']', 50))) SETTINGS max_parser_depth = 0;
"

# Brackets that do not match within a statement of a multi-statement input.
for script in "SELECT (1; SELECT 2)" "SELECT 1; SELECT (2" "SELECT (1; SELECT 2]" "SELECT 1); SELECT (2" "SELECT [1, (2]; SELECT 3" "SELECT 1; SELECT 2; SELECT (3))"; do
    echo "$script: $($CLICKHOUSE_FORMAT -n --query "$script" 2>&1 | grep -o -E "Code: [0-9]+|Unmatched parentheses: [^.]*|failed at position [0-9]+" | tr '\n' ' ')"
done

# Whether the syntax error lists DoubleColon among the expected tokens.
for query in "SELECT [1, 2] 3" "SELECT ((1, 2) 3)" "SELECT (1, (2, x) 3)" "SELECT [[1, 2], [3, x] 3]" "SELECT (1) 3" "SELECT [0x1] 3" "SELECT [0x1p1] 3" \
    "SELECT ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] 3, x)" \
    "SELECT ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, x] 3, x)"; do
    echo "$query: $($CLICKHOUSE_FORMAT --query "$query" 2>&1 | grep -c DoubleColon)"
done
