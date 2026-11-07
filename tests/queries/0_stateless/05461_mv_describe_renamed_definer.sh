#!/usr/bin/env bash

# `DESCRIBE` of a materialized view shows its own declared columns, so it must not need the definer of the view,
# which the view uses to refresh the external metadata of its target - directly and through an `Alias`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_definer_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS ${user}, ${user}_renamed;
CREATE USER ${user};
GRANT SELECT, INSERT ON *.* TO ${user};
CREATE TABLE src (x UInt64) ENGINE = Null;
CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv TO dst DEFINER = ${user} SQL SECURITY DEFINER AS SELECT x FROM src;
CREATE TABLE alias_to_mv ENGINE = Alias(mv);
ALTER USER ${user} RENAME TO ${user}_renamed;
"

$CLICKHOUSE_CLIENT -q "DESCRIBE mv"
$CLICKHOUSE_CLIENT -q "DESCRIBE alias_to_mv"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE alias_to_mv;
DROP TABLE mv;
DROP USER ${user}_renamed;
"
