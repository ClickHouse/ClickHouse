#!/usr/bin/env bash

# A user granted only the Alias table must get ACCESS_DENIED, not an error about the target's projection settings.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05621_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS ${user};
CREATE TABLE t_target (a UInt64, b UInt64, PROJECTION p (SELECT b ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS lightweight_mutation_projection_mode = 'throw';
INSERT INTO t_target VALUES (1, 10), (2, 20);
CREATE TABLE t_alias ENGINE = Alias(t_target);
CREATE USER ${user} NOT IDENTIFIED;
GRANT ALTER ON ${CLICKHOUSE_DATABASE}.t_alias TO ${user};
"

$CLICKHOUSE_CLIENT --user "${user}" -q "DELETE FROM ${CLICKHOUSE_DATABASE}.t_alias WHERE a = 1" 2>&1 | grep -o -m1 'ACCESS_DENIED'

# The whole `ALTER` group that `StorageAlias` checks on the target is not grantable on a single table.
$CLICKHOUSE_CLIENT -q "GRANT ALTER ON *.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "DELETE FROM ${CLICKHOUSE_DATABASE}.t_alias WHERE a = 1" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'

$CLICKHOUSE_CLIENT -q "
SELECT 'rows', count() FROM t_target;
DROP USER ${user};
DROP TABLE t_alias;
DROP TABLE t_target;
"
