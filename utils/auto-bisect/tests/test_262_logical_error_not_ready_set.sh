#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -mn -q "
SELECT version();

DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    col1 String,
    col2 Date,
    col3 String,
    col4 String,
    col5 Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (col4, col3);

INSERT INTO t1
SELECT
    'val' || toString(number % 282000),
    toDate('1970-01-01') + toIntervalDay(number % 20490),
    ['A', 'B', 'C', 'D'][number % 4 + 1],
    ['X', 'Y', 'Z'][number % 3 + 1],
    'val' || toString(number % 1000)
FROM numbers(1000000);

WITH base AS (
    SELECT col1, col5
    FROM (
        SELECT
            col1, col5,
            row_number() OVER (PARTITION BY col1) AS rn
        FROM t1
        WHERE col1 IN (
            SELECT col1 FROM t1
            WHERE col2 = toDate('2025-04-01')
        )
    ) AS __dedup
    WHERE rn = 1
      AND col1 IN (
          SELECT col1 FROM t1
          WHERE col3 IN ('no_such_val')
      )
)
SELECT *
FROM base AS d
CROSS JOIN base AS r
LIMIT 1;

"
