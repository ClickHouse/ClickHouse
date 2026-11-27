#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `SQL SECURITY DEFINER` or `NONE` view that only projects stored columns of a `MergeTree` table is transparent:
# its plan is a part of the invoker's plan, so the outer filter still reaches the primary key. A user who has only
# `SELECT` on the view must not learn from `EXPLAIN` what the view reads: the read is shown under the name of
# the view, without the names of the table, its skip indexes and its projections. The definer sees the real plan.

db=${CLICKHOUSE_DATABASE}
definer="definer_${db}_$RANDOM"
reader="reader_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
DROP USER IF EXISTS $definer, $reader;
CREATE USER $definer;
CREATE USER $reader;

CREATE TABLE $db.private_src
(
    x UInt8,
    s String,
    INDEX idx_private s TYPE bloom_filter GRANULARITY 1,
    PROJECTION proj_private (SELECT s, count() GROUP BY s)
)
ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.private_src VALUES (1, 'a');
GRANT SELECT ON $db.private_src TO $definer;

CREATE VIEW $db.definer_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT x, s FROM $db.private_src;
CREATE VIEW $db.none_view SQL SECURITY NONE AS SELECT x, s FROM $db.private_src;

GRANT SELECT ON $db.definer_view TO $definer, $reader;
GRANT SELECT ON $db.none_view TO $definer, $reader;
EOSQL

private_names='private_src\|idx_private\|proj_private'

# Whether the output of the query has a line matching the pattern.
has() {
    local user=$1 pattern=$2 query=$3
    if ${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1 | grep -q "$pattern"; then echo 1; else echo 0; fi
}

for view in definer_view none_view; do
    echo "-- $view: the reader sees no private name"
    for query in \
        "EXPLAIN SELECT * FROM $db.$view" \
        "EXPLAIN actions = 1, indexes = 1, projections = 1 SELECT * FROM $db.$view WHERE x = 1 AND s = 'a'" \
        "EXPLAIN json = 1, indexes = 1, projections = 1 SELECT * FROM $db.$view WHERE x = 1 AND s = 'a'" \
        "EXPLAIN indexes = 1, projections = 1 SELECT s, count() FROM $db.$view GROUP BY s" \
        "EXPLAIN indexes = 1, projections = 1 SELECT count() FROM $db.$view" \
        "EXPLAIN PLAN pretty = 1 SELECT * FROM $db.$view WHERE x = 1" \
        "EXPLAIN PIPELINE graph = 1 SELECT * FROM $db.$view" \
        "EXPLAIN ESTIMATE SELECT * FROM $db.$view"; do
        has "$reader" "$private_names" "$query"
    done

    echo "-- $view: the read is shown under the name of the view, and the outer filter still reaches the primary key"
    has "$reader" "ReadFromMergeTree ($db.$view)" "EXPLAIN SELECT * FROM $db.$view"
    has "$reader" "PrimaryKey" "EXPLAIN indexes = 1 SELECT * FROM $db.$view WHERE x = 1"
    ${CLICKHOUSE_CLIENT} --user "$reader" --query "EXPLAIN ESTIMATE SELECT * FROM $db.$view" | cut -f 2

    echo "-- $view: the reader still queries the view, but not the table"
    ${CLICKHOUSE_CLIENT} --user "$reader" --query "SELECT * FROM $db.$view WHERE x = 1"
    ${CLICKHOUSE_CLIENT} --user "$reader" --query "SELECT s, count() FROM $db.$view GROUP BY s"
done
${CLICKHOUSE_CLIENT} --user "$reader" --query "SELECT * FROM $db.private_src" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "-- the definer sees the table"
has "$definer" "ReadFromMergeTree ($db.private_src)" "EXPLAIN SELECT * FROM $db.definer_view"
${CLICKHOUSE_CLIENT} --user "$definer" --query "EXPLAIN ESTIMATE SELECT * FROM $db.definer_view" | cut -f 2

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.definer_view; DROP VIEW $db.none_view; DROP TABLE $db.private_src; DROP USER $definer, $reader"
