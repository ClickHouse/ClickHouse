#!/usr/bin/env bash

# The `minmax_<column>` columns of `mergeTreeIndex(..., with_minmax = 1)` are derived from the partition key column (or
# subcolumn) `<column>`, so a column-level grant on that column must be enough to read them, and a grant on another
# column must not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05453_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE mt_plain (k UInt64, p UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO mt_plain VALUES (1, 3);
CREATE TABLE mt_sub (k UInt64, t Tuple(a UInt64, b String)) ENGINE = MergeTree PARTITION BY t.a ORDER BY k;
INSERT INTO mt_sub VALUES (1, (5, 'x'));
CREATE TABLE mt_collision (k UInt64, t Tuple(a UInt64), minmax_t Tuple(a UInt64)) ENGINE = MergeTree PARTITION BY t.a ORDER BY k;
INSERT INTO mt_collision VALUES (1, (12345), (0));
CREATE USER $user;
GRANT SELECT(k) ON $CLICKHOUSE_DATABASE.mt_plain TO $user;
GRANT SELECT(k) ON $CLICKHOUSE_DATABASE.mt_sub TO $user;
GRANT SELECT(k, minmax_t) ON $CLICKHOUSE_DATABASE.mt_collision TO $user;
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1); then
        echo "OK: $output"
    elif grep -q "ACCESS_DENIED" <<< "$output"; then
        echo "ACCESS_DENIED"
    else
        echo "$output" | grep -o 'Code: [0-9]*' | head -n1
    fi
}

echo "-- a grant on another column does not cover the min-max of the partition key"
run_as_user "SELECT DISTINCT minmax_p FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_plain', with_minmax = 1)"
run_as_user "SELECT DISTINCT \`minmax_t.a\` FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_sub', with_minmax = 1)"

echo "-- a source column with the same name as the min-max output does not cover it"
run_as_user "SELECT DISTINCT \`minmax_t.a\` FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_collision', with_minmax = 1)"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $CLICKHOUSE_DATABASE.mt_plain TO $user; GRANT SELECT(t) ON $CLICKHOUSE_DATABASE.mt_sub TO $user; GRANT SELECT(t) ON $CLICKHOUSE_DATABASE.mt_collision TO $user;"

echo "-- a grant on the partition key column covers its min-max, also when the key is a subcolumn"
run_as_user "SELECT DISTINCT minmax_p FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_plain', with_minmax = 1)"
run_as_user "SELECT DISTINCT \`minmax_t.a\` FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_sub', with_minmax = 1)"
run_as_user "SELECT DISTINCT \`minmax_t.a\` FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt_collision', with_minmax = 1)"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
