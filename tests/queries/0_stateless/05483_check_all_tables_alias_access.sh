#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL='fatal'

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="test_05483_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT "
CREATE TABLE t_target (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_target VALUES ('x');
CREATE TABLE t_alias ENGINE = Alias(t_target);

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SHOW DATABASES ON *.* TO $user;
GRANT CHECK ON ${CLICKHOUSE_DATABASE}.t_alias TO $user;
"

# CHECK on an alias requires CHECK on its target as well.
$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_alias" 2>&1 | grep -o -F 'ACCESS_DENIED'
# CHECK ALL TABLES skips such an alias without resolving its target, instead of failing on it.
$CLICKHOUSE_CLIENT --user "$user" "CHECK ALL TABLES SETTINGS check_query_single_value_result = 0" 2>&1 | wc -l

$CLICKHOUSE_CLIENT "GRANT CHECK ON ${CLICKHOUSE_DATABASE}.t_target TO $user"

$CLICKHOUSE_CLIENT --user "$user" "CHECK TABLE t_alias SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT --user "$user" "CHECK ALL TABLES SETTINGS check_query_single_value_result = 0" 2>&1 | sed "s/^${CLICKHOUSE_DATABASE}\t/db\t/" | sort

$CLICKHOUSE_CLIENT "
DROP USER $user;
DROP TABLE t_alias;
DROP TABLE t_target;
"
