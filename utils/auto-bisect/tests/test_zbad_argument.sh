#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()";

$CH_PATH client -mn -q "
CREATE DATABASE x;
use x;
CREATE TABLE test
(
    x UInt64
)
ENGINE = SharedMergeTree
ORDER BY x SETTINGS storage_policy='s3_with_keeper';

INSERT INTO test SELECT number FROM numbers(1000000);
SELECT COUNT() FROM test;
ALTER TABLE test DETACH PARTITION tuple();
ALTER TABLE test ATTACH PARTITION tuple();
"
