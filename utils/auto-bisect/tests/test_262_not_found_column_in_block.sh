#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}


$CH_PATH client -mn -q "
SELECT version();
 drop table if exists default.table;
 CREATE TABLE default.table
(
    currency Nullable(String),

    col1 Nullable(String),
    INDEX idx_currency currency TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO default.table select * from generateRandom() limit 1000;

WITH RawTotals AS (
  SELECT
   col1 AS cta
  FROM
    default.table
),
Medians AS (
  SELECT
   col1 AS cta
  FROM
    default.table
  WHERE
    col1 IS NOT NULL
)
SELECT *
FROM (
  SELECT *, 'Raw Totals' AS calculation_type FROM RawTotals
  UNION ALL
  SELECT *, 'Median' AS calculation_type FROM Medians
) combined
WHERE calculation_type = 'Raw Totals';
"
