#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped

# A session's temporary table needs no grants, so neither does a hypothetical index or projection on it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05624_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;
GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user};
GRANT TABLE ENGINE ON MergeTree TO ${user};
"

$CLICKHOUSE_CLIENT --user "${user}" -q "
CREATE TEMPORARY TABLE tmp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tmp SELECT number, number FROM numbers(100);
CREATE HYPOTHETICAL INDEX idx ON tmp (v) TYPE minmax GRANULARITY 1;
CREATE HYPOTHETICAL PROJECTION p ON tmp (SELECT * ORDER BY v);
SELECT 'created';
DROP HYPOTHETICAL INDEX idx ON tmp;
DROP HYPOTHETICAL PROJECTION p ON tmp;
SELECT 'dropped';
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
