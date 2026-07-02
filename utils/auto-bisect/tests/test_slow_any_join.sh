#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

(
$CH_PATH client -mn -q "select version()";
if ! output=$($CH_PATH client -mn -q "
    SELECT count()
    FROM
    (
        SELECT
            analytics.created_at AS created_at,
            analytics.type AS type,
            analytics.toolkit_g AS toolkit_g,
            base_toolkits.name AS toolkit_name
        FROM
        (
            SELECT *
            FROM VALUES('created_at DateTime64(6), type LowCardinality(String), toolkit_g UUID, post_id UUID', ('1993-01-01', 'x', '75d076fd-84f3-4684-b830-090eca3d33a3', '75d076fd-84f3-4684-b830-090eca3d33a3'))
        ) AS analytics
        ANY LEFT JOIN
        (
            SELECT *
            FROM VALUES('id UUID, name String', ('75d076fd-84f3-4684-b830-090eca3d33a3', 'x'))
        ) AS base_toolkits ON base_toolkits.id = analytics.toolkit_g
        ANY LEFT JOIN
        (
            SELECT *
            FROM VALUES('id UUID', '75d076fd-84f3-4684-b830-090eca3d33a3')
        ) AS pppp ON pppp.id = analytics.post_id
        WHERE type IN ('share')
    ) AS t
    WHERE (created_at > 0) AND (length(toolkit_name) > 0)
    FORMAT NULL
    SETTINGS log_comment = 'x';
    -- We do not need to query system.query_log because we expect the main query to crash
" 2>&1); then
    echo "Test Passed: Query failed as expected."
    exit 0
else
    echo "Test Failed: Query unexpectedly succeeded."
    echo "Output: $output"
    exit 1
fi
)
