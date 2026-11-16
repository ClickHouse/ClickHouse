#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eE
set -o pipefail
trap 'printf "Failed at line %s: %s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

check_format()
{
    local expected="$1"
    local actual
    shift
    actual=$($CLICKHOUSE_FORMAT "$@")
    if [[ "$actual" != "$expected" ]]; then
        printf 'Formatting mismatch: %s\n' "$*" >&2
        diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2
        return 1
    fi
}

queries=$(cat <<'SQL'
SELECT user_id FROM events WHERE user_id IN (SELECT user_id FROM active_users WHERE enabled = 1) AND status = 'active';
SELECT column_name_longer_than_the_requested_wrapping_width FROM events WHERE user_id IN (SELECT user_id FROM active_users WHERE enabled = 1) AND status = 'active';
SELECT user_id FROM events WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
SELECT arrayMap(x -> concat(x, '_first_suffix', '_second_suffix', '_third_suffix'), labels) FROM events;
SELECT (a, b, c) > (first_long_identifier, second_long_identifier, third_long_identifier) FROM events;
ALTER TABLE events UPDATE status = if(user_id IN (1, 2, 3), 'enabled', 'disabled') WHERE event_date >= '2026-01-01' AND user_id = 123456789;
SELECT sum(value) OVER w FROM events WINDOW w AS (PARTITION BY customer_identifier, region_identifier ORDER BY event_timestamp, sequence_number);
SELECT sum(value) OVER (PARTITION BY customer_identifier, region_identifier ORDER BY event_timestamp, sequence_number) FROM events;
SELECT very_long_left_operand + sum(value) OVER (PARTITION BY customer_identifier, region_identifier ORDER BY event_timestamp, sequence_number) FROM events;
SELECT user_id FROM events WHERE (first_long_column = 1 OR second_long_column = 2) AND (third_long_column = 3 OR fourth_long_column = 4);
SELECT f(first_parameter, second_parameter, third_parameter)(first_argument, second_argument) FROM events;
SELECT very_long_function_name((WITH active_people AS (SELECT first_name FROM people WHERE enabled = 1) SELECT first_name FROM active_people), another_long_argument, third_long_argument);
SELECT f((SELECT 1), first_very_long_argument, second_very_long_argument, (SELECT 2), z);
SELECT (first_long_identifier, second_long_identifier) = (SELECT (first_column, second_column) FROM values_table WHERE enabled = 1);
SELECT very_long_outer_operand_identifier + (SELECT people.value FROM people INNER JOIN labels ON people.id = labels.id WHERE labels.enabled = 1);
INSERT INTO events (first_long_column, second_long_column, third_long_column) SELECT 1, 2, 3;
SQL
)

original=$($CLICKHOUSE_FORMAT --multiquery --semicolons_inline --oneline --query "$queries")
for width in 40 60; do
    echo "Wrapping width $width"
    formatted=$($CLICKHOUSE_FORMAT --multiquery --semicolons_inline --wrap_width "$width" --query "$queries")
    printf '%s\n' "$formatted"
    check_format "$original" --multiquery --semicolons_inline --oneline --query "$formatted"
    check_format "$formatted" --multiquery --semicolons_inline --wrap_width "$width" --query "$formatted"
done
