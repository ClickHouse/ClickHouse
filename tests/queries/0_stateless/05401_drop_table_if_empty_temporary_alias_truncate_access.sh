#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TRUNCATE TABLE ... IF EMPTY` of a temporary `Alias` truncates its target, so, as for an ordinary
# `Alias`, it checks the privilege to truncate the target before it looks at (and loads) the target:
# otherwise a user who may not truncate the target could tell an empty target (`ACCESS_DENIED`)
# from a non-empty one (`TABLE_NOT_EMPTY`).

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE t_full (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_full VALUES (1);
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user};
    GRANT SHOW COLUMNS ON ${db}.t_full TO ${user};
    GRANT SHOW COLUMNS ON ${db}.t_empty TO ${user};
"

function truncate_alias()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        CREATE TEMPORARY TABLE tmp_alias ENGINE = Alias('${db}', '$1');
        TRUNCATE TABLE IF EMPTY tmp_alias;
        SELECT 'truncated';
    " 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|TABLE_NOT_EMPTY|truncated'
}

truncate_alias t_full
truncate_alias t_empty

${CLICKHOUSE_CLIENT} --query "
    GRANT TRUNCATE ON ${db}.t_full TO ${user};
    GRANT TRUNCATE ON ${db}.t_empty TO ${user};
"

truncate_alias t_full
truncate_alias t_empty

${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_full;
    DROP USER ${user};
"
