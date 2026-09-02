#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

VIEW_NAME="x"

$CH_PATH client -mn -q "
drop table if exists tttt;
CREATE TABLE IF NOT EXISTS tttt
(
    wwww UInt32,
    qqqq1 DateTime64(3),
    eeeee DateTime64(3),
    rrrrr Decimal32(2),
    ttttttt Decimal32(2)
)
ENGINE = ReplacingMergeTree(eeeee)
PARTITION BY toYYYYMM(qqqq1)
ORDER BY (wwww, qqqq1);


CREATE OR REPLACE VIEW $VIEW_NAME
AS SELECT
    wwww,
    qqqq1,
    rrrrr
FROM tttt
FINAL
COMMENT 'comment'
;
"

# Extract create_table_query and calculate positions
# We use position() inside ClickHouse for a cleaner, high-performance check
RESULT=$($CH_PATH client -q "
SELECT
    if(position(create_table_query, 'COMMENT') < position(create_table_query, 'AS SELECT'), 1, 0)
FROM system.tables
WHERE name = '$VIEW_NAME'
")

if [ "$RESULT" -eq "1" ]; then
    echo "Logic Match: COMMENT is at the beginning. Exiting with 1."
    exit 1
else
    echo "Logic Match: COMMENT is at the end. Exiting with 0."
    exit 0
fi
