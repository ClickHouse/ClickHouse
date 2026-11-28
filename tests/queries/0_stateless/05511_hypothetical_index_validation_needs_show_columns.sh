#!/usr/bin/env bash
# the validation of a hypothetical index checks the whole table, so its errors can name any column

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05511_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE t_show (public UInt64, j JSON(secret UInt32), INDEX j_idx j TYPE minmax) ENGINE = MergeTree ORDER BY public
        SETTINGS allow_minmax_index_for_json = 1;
    ALTER TABLE t_show RESET SETTING allow_minmax_index_for_json;
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT(public) ON ${CLICKHOUSE_DATABASE}.t_show TO ${user};
"

echo "without SHOW COLUMNS the error of another index is not shown"
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL INDEX i_public ON ${CLICKHOUSE_DATABASE}.t_show (public) TYPE minmax" 2>&1 \
    | grep -o -E 'ACCESS_DENIED|secret' | sort -u

echo "with SHOW COLUMNS the error is the one of ALTER TABLE ADD INDEX"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_show TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE HYPOTHETICAL INDEX i_public ON ${CLICKHOUSE_DATABASE}.t_show (public) TYPE minmax" 2>&1 \
    | grep -o -m1 -E 'ACCESS_DENIED|BAD_ARGUMENTS'

$CLICKHOUSE_CLIENT -q "DROP USER ${user}; DROP TABLE t_show;"
