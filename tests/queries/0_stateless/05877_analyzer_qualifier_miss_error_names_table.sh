#!/usr/bin/env bash
# An identifier whose qualifier names a table expression that has no such column is reported
# against that table expression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${DB}.${DB} (id Int32) ENGINE = MergeTree ORDER BY ();
CREATE TABLE ${DB}.tbl (id Int32) ENGINE = MergeTree ORDER BY ();
"

message()
{
    $CLICKHOUSE_CLIENT --enable_analyzer 1 --query "$1" 2>&1 \
        | grep -m1 -F 'cannot be resolved from' \
        | sed -e 's/.*\(cannot be resolved from .*\)\. In scope.*/\1/' -e 's/\. Maybe you meant.*//' -e "s/${DB}/db/g"
}

# The table name equals the database name: `db.tbl.nope` is the database-qualified column of `db.tbl`.
message "SELECT ${DB}.tbl.nope FROM ${DB}.${DB} JOIN ${DB}.tbl USING (id)"

# The qualifier is the alias.
message "SELECT a.nope FROM ${DB}.tbl AS a JOIN ${DB}.${DB} AS b USING (id)"

# The qualifier is the table name, the table has another alias.
message "SELECT one.x FROM system.one AS o"

# The qualifier is the alias of a subquery.
message "SELECT s.nope FROM (SELECT 1 AS x) AS s"
