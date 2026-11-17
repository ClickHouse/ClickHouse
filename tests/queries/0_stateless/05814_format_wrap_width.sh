#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

format()
{
    $CLICKHOUSE_FORMAT --multiquery --semicolons_inline "$@"
}

layout_queries=$(cat <<'SQL'
SELECT f(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);
SELECT f(g(first_argument), g(second_argument), third_argument) FROM numbers(1);
SELECT number, concat('first', 'second', 'third', 'fourth') AS result FROM numbers(1);
SELECT concat('spaces inside a very long string', 'x') AS `spaces inside a very long identifier`;
SELECT concat('你好', '世界', '🙂', 'é', '乌龟走着走着，找到了辣椒');
SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], tuple(tuple(), tuple(1), tuple(1, tuple(2, 3)));
SELECT user_id FROM events WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
SELECT user_id FROM events WHERE user_id IN (SELECT user_id FROM active_users WHERE enabled = 1) AND status = 'active';
SELECT user_id FROM events WHERE (first_long_column = 1 OR second_long_column = 2) AND (third_long_column = 3 OR fourth_long_column = 4);
SELECT arrayMap(x -> concat(x, '_first_suffix', '_second_suffix', '_third_suffix'), labels) FROM events;
SELECT (a, b, c) > (first_long_identifier, second_long_identifier, third_long_identifier) FROM events;
SELECT f(first_parameter, second_parameter, third_parameter)(first_argument, second_argument) FROM events;
SELECT sum(value) OVER (PARTITION BY customer_identifier, region_identifier ORDER BY event_timestamp, sequence_number) FROM events;
SELECT sum(value) OVER w FROM events WINDOW w AS (PARTITION BY customer_identifier ORDER BY event_timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
SELECT a.value FROM first_table AS a INNER JOIN second_table AS b ON a.identifier = b.identifier AND a.tenant_id = b.tenant_id;
SELECT very_long_function_name((WITH people AS (SELECT name FROM persons WHERE enabled = 1) SELECT name FROM people), another_argument);
INSERT INTO events (first_long_column, second_long_column, third_long_column) SELECT 1, 2, 3;
INSERT INTO a_long_table_name (a_long_column_name, another_long_column_name) VALUES ('spaces inside payload', 123);
ALTER TABLE events UPDATE status = if(user_id IN (1, 2, 3), 'enabled', 'disabled') WHERE event_date >= '2026-01-01' AND user_id = 123456789;
SQL
)

format --wrap_width 40 --query "$layout_queries"

echo 'Backslash'
$CLICKHOUSE_FORMAT --wrap_width 24 --backslash --query "SELECT concat('first', 'second', 'third')"

echo 'Separate semicolons'
$CLICKHOUSE_FORMAT --wrap_width 24 --multiquery --query "SELECT concat('first', 'second');"

echo 'Single line takes precedence'
$CLICKHOUSE_FORMAT --wrap_width 1 --oneline --query "SELECT concat('first', 'second')"
$CLICKHOUSE_FORMAT --wrap_width 1 --max_line_length 255 --query "SELECT concat('first', 'second')"

# Invariants: wrapping changes only whitespace, is idempotent, and does nothing when everything fits.
invariant_queries=$(cat <<'SQL'
SELECT map('first', tuple(1, 2), 'second', tuple(3, 4)), [toUInt8(1), toUInt8(2)], array(), [], map(), tuple(), tuple(1);
SELECT [-1, -2, -3], array(-1, -2, -3), f()(a, b, c, d, e, f, g, h, i, j, k), quantiles(0.25, 0.5, 0.75)(number);
SELECT (number + 1) * (number + 2) AS value FROM numbers(3) WHERE number = 0 OR number = 1 AND number < 3;
SELECT position((1 IN (SELECT 1)), 1), CAST('123' AS Nullable(UInt64)), (f | g) | h, f | (g | h);
SELECT concat('escaped \' quote', 'tab\there', 'line\nbreak', $doc$white space, 'quotes'$doc$, 'multi
line');
SELECT (concat('first', 'second') AS alias), concat('first', 'second') AS alias, 1 AS `first
second`;
SELECT {p:Tuple(first String, second Array(Tuple(value UInt64, label String)))}, {key:String};
SELECT toString(number) NOT SIMILAR TO '[0-9]!_%' ESCAPE '!' FROM numbers(10);
SELECT `a..b`.f(concat('first', 'second'), concat('third', 'fourth'));
SELECT * FROM (SELECT 1 AS value) UNION ALL SELECT 2 SETTINGS max_threads = 1;
SQL
)
queries="$layout_queries"$'\n'"$invariant_queries"
oneline=$(format --show_secrets --oneline --query "$queries")
unwrapped=$(format --show_secrets --query "$queries")
for width in 1 16 40 120; do
    formatted=$(format --show_secrets --wrap_width "$width" --query "$queries")
    [[ $(format --show_secrets --oneline --query "$formatted") == "$oneline" ]] || echo "Wrapping at $width changed the query"
    [[ $(format --show_secrets --wrap_width "$width" --query "$formatted") == "$formatted" ]] || echo "Wrapping at $width is not idempotent"
    [[ $(format --wrap_width "$width" --query "SELECT * FROM url(url_creds, url = 'https://example.com/?token=secret'), mysql('host', 'db', 'table', 'user', 'secret')") != *secret* ]] || echo "Wrapping at $width shows secrets"
done
[[ $(format --show_secrets --wrap_width 1000 --query "$queries") == "$unwrapped" ]] || echo 'Wrapping changed queries that fit'
[[ $(format --show_secrets --wrap_width 0 --query "$queries") == "$unwrapped" ]] || echo 'Width 0 does not disable wrapping'

for width in -1 1.5 invalid 18446744073709551616; do
    if $CLICKHOUSE_FORMAT --wrap_width "$width" --query 'SELECT 1' > /dev/null 2>&1; then
        echo "Width $width accepted"
    fi
done

query="SELECT concat('first', 'second', 'third', 'fourth')"
formatted=$($CLICKHOUSE_FORMAT --wrap_width 24 --query "$query")
[[ $($CLICKHOUSE_FORMAT --wrap_width 24 --hilite --query "$query" | sed -E $'s/\033\\[[0-9;]*m//g') == "$formatted" ]] || echo 'Highlighting changed the layout'

# Deep nesting must not make the output grow quadratically.
python3 -c "print('SELECT ' + '[' * 3000 + '1' + ']' * 3000)" | $CLICKHOUSE_FORMAT --wrap_width 40 --max_parser_depth 10000 | wc -c | awk '{ print ($1 < 200000) ? "Deep nesting OK" : "Deep nesting output is too large: " $1 }'
