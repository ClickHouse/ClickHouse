#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

queries=$(cat <<'EOF'
SELECT f(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z);
SELECT f(g(first_argument), g(second_argument), third_argument) FROM numbers(1);
SELECT number, concat('first', 'second', 'third', 'fourth') AS result FROM numbers(1);
SELECT concat('spaces inside a very long string', 'x') AS `spaces inside a very long identifier`;
SELECT concat('你好', '世界', '🙂', 'é', '乌龟走着走着，找到了辣椒');
SELECT [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], tuple(tuple(), tuple(1), tuple(1, tuple(2, 3)));
SELECT user_id FROM events WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
SELECT user_id FROM events WHERE (first_long_column = 1 OR second_long_column = 2) AND (third_long_column = 3 OR fourth_long_column = 4);
SELECT arrayMap(x -> concat(x, '_first_suffix', '_second_suffix', '_third_suffix'), labels) FROM events;
SELECT (a, b, c) > (first_long_identifier, second_long_identifier, third_long_identifier) FROM events;
SELECT f(first_parameter, second_parameter, third_parameter)(first_argument, second_argument) FROM events;
SELECT sum(value) OVER w FROM events WINDOW w AS (PARTITION BY customer_identifier ORDER BY event_timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
SELECT a.value FROM first_table AS a INNER JOIN second_table AS b ON a.identifier = b.identifier AND a.tenant_id = b.tenant_id;
SELECT f((SELECT name FROM persons WHERE enabled = 1), first_long_argument, second_long_argument);
INSERT INTO events (first_long_column, second_long_column, third_long_column) SELECT 1, 2, 3;
ALTER TABLE events UPDATE status = if(user_id IN (1, 2, 3), 'enabled', 'disabled') WHERE event_date >= '2026-01-01' AND user_id = 123456789;
EOF
)

echo "--- wrap_width 40"
$CLICKHOUSE_FORMAT -n --semicolons_inline --wrap_width 40 --query "$queries"

echo "--- nesting is not wrapped deeper than the width"
$CLICKHOUSE_FORMAT --wrap_width 12 --query "SELECT [[[[[[1, 2]]]]]]"

echo "--- backslash"
$CLICKHOUSE_FORMAT --wrap_width 24 --backslash --query "SELECT concat('first', 'second', 'third')"

echo "--- semicolons on separate lines"
$CLICKHOUSE_FORMAT -n --wrap_width 24 --query "SELECT concat('first', 'second');"

echo "--- one line takes precedence"
$CLICKHOUSE_FORMAT --wrap_width 1 --oneline --query "SELECT concat('first', 'second')"
$CLICKHOUSE_FORMAT --wrap_width 1 --max_line_length 255 --query "SELECT concat('first', 'second')"

# Wrapping changes only whitespace, gives the same result when applied again and does not change queries that already fit.
queries+=$(cat <<'EOF'

SELECT map('first', tuple(1, 2), 'second', tuple(3, 4)), [toUInt8(1), toUInt8(2)], array(), [], map(), tuple(), tuple(1);
SELECT [-1, -2, -3], array(-1, -2, -3), f()(a, b, c, d, e, f, g, h, i, j, k), quantiles(0.25, 0.5, 0.75)(number);
SELECT (number + 1) * (number + 2) AS value FROM numbers(3) WHERE number = 0 OR number = 1 AND number < 3;
SELECT position((1 IN (SELECT 1)), 1), CAST('123' AS Nullable(UInt64)), (f | g) | h, f | (g | h);
SELECT concat('escaped \' quote', 'tab\there', 'line\nbreak', $doc$white space, 'quotes'$doc$, 'multi
line'), 1 AS `first
second`;
SELECT {p:Tuple(first String, second Array(Tuple(value UInt64, label String)))}, {key:String};
SELECT toString(number) NOT SIMILAR TO '[0-9]!_%' ESCAPE '!' FROM numbers(10);
SELECT sum(value) OVER (PARTITION BY customer_identifier ORDER BY event_timestamp) FROM events;
SELECT * FROM (SELECT 1 AS value) UNION ALL SELECT 2 SETTINGS max_threads = 1;
EOF
)
oneline=$($CLICKHOUSE_FORMAT -n --show_secrets --oneline --query "$queries")
unwrapped=$($CLICKHOUSE_FORMAT -n --show_secrets --query "$queries")
for width in 10 16 40 120; do
    wrapped=$($CLICKHOUSE_FORMAT -n --show_secrets --wrap_width "$width" --query "$queries")
    [ "$($CLICKHOUSE_FORMAT -n --show_secrets --oneline --query "$wrapped")" == "$oneline" ] || echo "query changed at width $width"
    [ "$($CLICKHOUSE_FORMAT -n --show_secrets --wrap_width "$width" --query "$wrapped")" == "$wrapped" ] || echo "not idempotent at width $width"
done
[ "$($CLICKHOUSE_FORMAT -n --show_secrets --wrap_width 1000 --query "$queries")" == "$unwrapped" ] || echo "queries that fit are changed"

# Hidden secrets do not affect the layout.
secrets="SELECT * FROM url(url_creds, url = 'https://example.com/?token=secret'), mysql('host', 'db', 'table', 'user', 'secret')"
for width in 10 40; do
    hidden=$($CLICKHOUSE_FORMAT --wrap_width "$width" --query "$secrets")
    [[ $hidden != *secret* ]] || echo "secret shown at width $width"
    [ "$($CLICKHOUSE_FORMAT --wrap_width "$width" --query "${secrets//secret/much_longer_secret}")" == "$hidden" ] || echo "layout depends on secrets at width $width"
done
