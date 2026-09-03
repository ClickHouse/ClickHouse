#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()";

$CH_PATH client -mn -q "
set execute_exists_as_scalar_subquery='false';

SELECT 1
FROM
(
    SELECT 1 AS c0
    WHERE exists((
        SELECT 1
    ))
    LIMIT 1
) AS v0
GROUP BY v0.c0
HAVING (v0.c0 = 1) AND (v0.c0 = 2)
SETTINGS exact_rows_before_limit = 1
"
