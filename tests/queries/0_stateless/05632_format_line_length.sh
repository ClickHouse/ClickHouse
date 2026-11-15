#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
set -o pipefail

check_width()
{
    printf '%s\n' "$2" | LC_ALL=C awk -v width="$1" '
        length($0) > width { print "Line exceeds " width " columns: " $0 > "/dev/stderr"; exit 1 }
        /[ \t]$/ { print "Trailing whitespace: " $0 > "/dev/stderr"; exit 1 }
    '
}

check_format()
{
    local expected="$1"
    local actual
    shift
    actual=$($CLICKHOUSE_FORMAT "$@")
    test "$actual" = "$expected"
}

query="SELECT multiIf(number = 0, 'zero', number = 1, 'one', number = 2, 'two', number = 3, 'three', number = 4, 'four', 'other') FROM numbers(6)"

echo 'Default width'
$CLICKHOUSE_FORMAT --query "$query"

echo 'Long expression'
arguments=$(printf 'a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,%.0s' {1..10})
formatted=$($CLICKHOUSE_FORMAT --query "SELECT f(${arguments%,})")
check_width 120 "$formatted"
original=$($CLICKHOUSE_FORMAT --oneline --query "SELECT f(${arguments%,})")
check_format "$original" --oneline --query "$formatted"
check_format "$formatted" --query "$formatted"
echo 'Long expression width OK'

echo 'Argument list'
$CLICKHOUSE_FORMAT --line_length 40 --query 'SELECT f(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)'

echo 'Narrow width'
$CLICKHOUSE_FORMAT --line_length 40 --query "$query"

echo 'Disabled wrapping'
$CLICKHOUSE_FORMAT --line_length 0 --query "$query"

echo 'Boundary widths'
for width in 22 23 24; do
    $CLICKHOUSE_FORMAT --line_length "$width" --query "SELECT concat('a', 'b')"
done

echo 'Existing indentation'
$CLICKHOUSE_FORMAT --line_length 32 --query "SELECT number, concat('first', 'second', 'third', 'fourth') AS result FROM numbers(1)"

echo 'Quoted tokens'
$CLICKHOUSE_FORMAT --line_length 24 --query "SELECT concat('spaces inside a very long string', 'x') AS \`spaces inside a very long identifier\`"

echo 'Unicode width'
$CLICKHOUSE_FORMAT --line_length 24 --query "SELECT concat('你好', '世界', '🙂', 'é')"

echo 'Unicode boundary widths'
# 33 display columns, 45 UTF-8 bytes, 21 code points.
query="SELECT '乌龟走着走着，找到了辣椒'"
for width in 36 33 32; do
    $CLICKHOUSE_FORMAT --line_length "$width" --query "$query"
done

echo 'Nested literals'
$CLICKHOUSE_FORMAT --line_length 40 --query 'SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], tuple(tuple(), tuple(1), tuple(1, tuple(2, 3)))'

echo 'Window clauses'
$CLICKHOUSE_FORMAT --line_length 40 --query 'SELECT first_value(number) IGNORE NULLS OVER (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM numbers(10)'

echo 'Single line takes precedence'
$CLICKHOUSE_FORMAT --line_length 1 --oneline --query "SELECT concat('first', 'second')"
$CLICKHOUSE_FORMAT --line_length 1 --max_line_length 255 --query "SELECT concat('first', 'second')"

echo 'Backslash continuation'
$CLICKHOUSE_FORMAT --line_length 24 --backslash --query "SELECT concat('first', 'second', 'third')"

echo 'Multiple queries'
$CLICKHOUSE_FORMAT --line_length 24 --multiquery --semicolons_inline --query "SELECT concat('first', 'second'); SELECT concat('third', 'fourth')"

echo 'Semicolons on separate lines'
$CLICKHOUSE_FORMAT --line_length 24 --multiquery --query "SELECT concat('first', 'second'); SELECT concat('third', 'fourth')"

echo 'Insert payload'
$CLICKHOUSE_FORMAT --line_length 24 --query "INSERT INTO a_long_table_name (a_long_column_name, another_long_column_name) VALUES ('spaces inside payload', 123)"

queries=
while IFS= read -r query; do
    queries+="$query;"$'\n'
done <<'SQL'
SELECT multiIf(number = 0, 'zero', number = 1, 'one', 'other') AS value FROM numbers(3)
SELECT arrayMap((x, y) -> concat(x, y), ['first', 'second'], ['third', 'fourth'])
SELECT map('first', tuple(1, 2), 'second', tuple(3, 4)), [toUInt8(1), toUInt8(2)]
SELECT (number + 1) * (number + 2) AS value FROM numbers(3) WHERE number = 0 OR number = 1 AND number < 3
SELECT quantiles(0.25, 0.5, 0.75)(number) FROM numbers(10)
SELECT first_value(number) IGNORE NULLS OVER (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM numbers(10)
SELECT position((1 IN (SELECT 1)), 1), CAST('123' AS Nullable(UInt64))
SELECT concat('你好', '世界', '🙂', 'é', 'Привет') AS `别名 with spaces`
SELECT concat('escaped \' quote', 'escaped \\ slash', 'tab\there', 'line\nbreak')
SELECT concat($doc$white space, 'quotes', and $$ markers$doc$, 'suffix')
SELECT (concat('first', 'second') AS alias), concat('first', 'second') AS alias
SELECT {long_parameter_name:Tuple(String, Array(UInt64))}
SELECT {parameter:Tuple(first String, second Array(Tuple(value UInt64, label String)))}
SELECT concat({first_parameter:String}, {second_parameter:String}) AS value
SELECT map({key:String}, {value:Tuple(`{key}` String, `x y` UInt64)})
SELECT tuple(), tuple(1), tuple(tuple(1), tuple(2, 3)), [[], [1, 2, 3]]
SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], ((1, 2), (3, 4))
SELECT number FROM numbers(1) WHERE long_column_id IS NOT NULL AND other_id IS NOT NULL
SELECT (f | g) | h, f | (g | h), __compose(f, g)
SELECT toString(number) NOT SIMILAR TO '[0-9]!_%' ESCAPE '!' FROM numbers(10)
SELECT `a..b`.f(concat('first', 'second'), concat('third', 'fourth'))
SQL

original=$($CLICKHOUSE_FORMAT --multiquery --semicolons_inline --oneline --show_secrets --query "$queries")
for width in 1 16 40 120; do
    formatted=$($CLICKHOUSE_FORMAT --multiquery --semicolons_inline --line_length "$width" --show_secrets --query "$queries")
    check_format "$formatted" --multiquery --semicolons_inline --line_length "$width" --show_secrets --query "$formatted"
    check_format "$original" --multiquery --semicolons_inline --oneline --show_secrets --query "$formatted"
done

for width in 1 2 4 13 16 40; do
    query='SELECT array(), [], map(), tuple(), tuple(1), array(array(), [1]), [-1, -2, -3], array(-1, -2, -3), f(g(123456), x)'
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query "$query")
    check_format "$formatted" --line_length "$width" --query "$formatted"
    original=$($CLICKHOUSE_FORMAT --oneline --query "$query")
    check_format "$original" --oneline --query "$formatted"
done
echo 'Round trips OK'

query='SELECT f()(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z)'
formatted=$($CLICKHOUSE_FORMAT --line_length 40 --query "$query")
check_width 40 "$formatted"
[[ $formatted == *'f()('* ]]
original=$($CLICKHOUSE_FORMAT --oneline --query "$query")
check_format "$original" --oneline --query "$formatted"
check_format "$formatted" --line_length 40 --query "$formatted"
echo 'Empty parameters OK'

while read -r depth width; do
    query='number'
    for ((i = 0; i < depth; ++i)); do
        query="toString($query)"
    done
    query="SELECT $query FROM numbers(1)"
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query "$query")
    check_width "$width" "$formatted"
    check_format "$formatted" --line_length "$width" --query "$formatted"
    original=$($CLICKHOUSE_FORMAT --oneline --query "$query")
    check_format "$original" --oneline --query "$formatted"
done <<'DEPTHS'
6 40
14 80
24 120
DEPTHS
echo 'Nested call widths OK'

for width in 32 40; do
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query "SELECT number, concat('first', 'second', 'third', 'fourth') AS result FROM numbers(1)")
    check_width "$width" "$formatted"
    [[ $formatted == *'AS result'* ]]
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query "SELECT multiIf(number = 0, 'zero', number = 1, 'one', number = 2, 'two', number = 3, 'three', number = 4, 'four', 'other') FROM numbers(6)")
    check_width "$width" "$formatted"
    for value in 0 1 2 3 4; do
        [[ $formatted == *"number = $value"* ]]
    done
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query 'SELECT number FROM numbers(1) WHERE long_column_id IS NOT NULL AND other_id IS NOT NULL')
    check_width "$width" "$formatted"
    [[ $formatted == *'IS NOT NULL'* ]]
    test "$(printf '%s\n' "$formatted" | grep -c 'IS NOT NULL')" -eq 2
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query 'SELECT first_value(number) IGNORE NULLS OVER (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM numbers(10)')
    [[ $formatted == *'IGNORE NULLS'* && $formatted == *'PARTITION BY'* && $formatted == *'ORDER BY'* && $formatted == *'CURRENT ROW'* ]]
    formatted=$($CLICKHOUSE_FORMAT --line_length "$width" --query "SELECT number GLOBAL NOT IN (1, 2, 3), toString(number) NOT LIKE '%3%' FROM numbers(4)")
    [[ $formatted == *'GLOBAL NOT IN'* && $formatted == *'NOT LIKE'* ]]
done
formatted=$($CLICKHOUSE_FORMAT --line_length 40 --query 'SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], ((1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (11, 12))')
check_width 40 "$formatted"
formatted=$($CLICKHOUSE_FORMAT --line_length 21 --query 'SELECT f(inner(123456), x)')
check_width 21 "$formatted"
echo 'Structural boundaries OK'

query="SELECT concat('first', 'second', 'third') AS result"
variant=$(printf "SELECT\nconcat( 'first',\t'second',\n 'third' )\nAS result")
for width in 24 40 120; do
    original=$($CLICKHOUSE_FORMAT --line_length "$width" --query "$query")
    check_format "$original" --line_length "$width" --query "$variant"
done
echo 'Input whitespace OK'

for query in \
    'SELECT f((SELECT 1))' \
    'SELECT f((SELECT 1), (SELECT 2))' \
    'SELECT * FROM (SELECT 1 AS value)' \
    'INSERT INTO t SELECT * FROM (SELECT 1 AS value)' \
    $'SELECT \'first\nsecond\'' \
    $'SELECT f(\'first\nsecond\')' \
    $'SELECT 1 AS `first\nsecond`' \
    $'SELECT f($doc$first\nsecond$doc$)' \
    $'SELECT {p:Tuple(first String,\nsecond Array(UInt64))}'; do
    unwrapped=$($CLICKHOUSE_FORMAT --line_length 0 --query "$query")
    check_width 120 "$unwrapped"
    check_format "$unwrapped" --query "$query"
    check_format "$unwrapped" --line_length 18446744073709551615 --query "$query"
done
echo 'Fitting output preserved'

for limit in -1 -2 -18446744073709551615 -0 1.5 invalid 18446744073709551616; do
    if $CLICKHOUSE_FORMAT --line_length="$limit" --query 'SELECT 1' > /dev/null 2>&1; then
        echo "Invalid line length accepted: $limit" >&2
        exit 1
    fi
done
original=$($CLICKHOUSE_FORMAT --line_length 18446744073709551615 --query "$query")
check_format "$original" --line_length 0 --query "$query"
echo 'Line length validation OK'

query="SELECT concat('first', 'second', 'third', 'fourth')"
formatted=$($CLICKHOUSE_FORMAT --line_length 24 --query "$query")
highlighted=$($CLICKHOUSE_FORMAT --line_length 24 --hilite --query "$query" | sed -E $'s/\033\\[[0-9;]*m//g')
backslash=$($CLICKHOUSE_FORMAT --line_length 24 --backslash --query "$query" | sed 's/ \\$//')
test "$formatted" = "$highlighted"
test "$formatted" = "$backslash"
echo 'Highlight and backslash OK'

long_secret=$(printf 'secret%.0s' {1..100})
hidden=$($CLICKHOUSE_FORMAT --line_length 40 --query "SELECT * FROM mysql('localhost', 'database', 'table', 'username', 'short_secret')")
check_format "$hidden" --line_length 40 --query "SELECT * FROM mysql('localhost', 'database', 'table', 'username', '$long_secret')"
[[ $hidden != *short_secret* && $hidden != *"$long_secret"* && $hidden == *'[HIDDEN]'* ]]
hidden=$($CLICKHOUSE_FORMAT --line_length 40 --query "CREATE TABLE t AS s3('http://127.0.0.1:1/x/', 'minio', 'short_secret')(1)")
check_format "$hidden" --line_length 40 --query "CREATE TABLE t AS s3('http://127.0.0.1:1/x/', 'minio', '$long_secret')(1)"
[[ $hidden != *short_secret* && $hidden != *"$long_secret"* ]]
test "$(printf '%s\n' "$hidden" | grep -o '\[HIDDEN\]' | wc -l)" -eq 4
for query in \
    "SELECT * FROM azureBlobStorage(azure_conf, storage_account_url = 'https://account.blob.core.windows.net/?sig=secret', container = 'container', blob_path = 'blob')" \
    "SELECT * FROM azureBlobStorage(azure_conf, container = headers('Authorization' = 'secret'), blob_path = 'blob')" \
    "SELECT * FROM url(url_creds, \`url\` = 'https://example.com/data.csv?token=secret&value=a = b')"; do
    for width in 1 24 40 120; do
        hidden=$($CLICKHOUSE_FORMAT --line_length "$width" --query "$query")
        [[ $hidden != *secret* && $hidden == *'[HIDDEN]'* ]]
        check_format "$hidden" --line_length "$width" --query "$hidden"
    done
done
echo 'Hidden secrets OK'

query=$(printf "SELECT concat('invalid \\377 byte', 'suffix')")
formatted=$($CLICKHOUSE_FORMAT --line_length 16 --query "$query")
original=$($CLICKHOUSE_FORMAT --oneline --query "$query")
check_format "$original" --oneline --query "$formatted"
check_format "$formatted" --line_length 16 --query "$formatted"
echo 'Invalid UTF-8 OK'
