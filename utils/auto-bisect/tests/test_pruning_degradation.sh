#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()"


$CH_PATH client -mn -q "
DROP TABLE IF EXISTS kek;
CREATE TABLE kek (id Int) ENGINE = Memory();
INSERT INTO kek VALUES (2);
"

RANDOM_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)

echo "RANDOM_ID: $RANDOM_ID"

$CH_PATH client -mn --query_id $RANDOM_ID -q "
SELECT * FROM icebergS3('http://localhost:11111/test/warehouse/default/test_pruning_degradation_1', 'clickhouse', 'clickhouse')
WHERE id IN (SELECT * FROM kek) SETTINGS use_iceberg_partition_pruning = 1;
"

Query="SYSTEM FLUSH LOGS; SELECT 
    ProfileEvents['IcebergPartitionPrunedFiles']
FROM system.query_log 
WHERE query_id == '$RANDOM_ID' AND type == 'QueryFinish';"

RESULT=$($CH_PATH client -mn -q "$Query")

echo "$RESULT"


if [ "$RESULT" -gt 0 ]; then
    echo "✓ Test passed: $RESULT > 0"
    exit 0
else
    echo "✗ Test failed: $RESULT <= 0"
    exit 1
fi