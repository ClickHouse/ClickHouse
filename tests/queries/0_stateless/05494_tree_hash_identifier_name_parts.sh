#!/usr/bin/env bash
# Table identifiers that differ only in where the dot falls must not share an AST tree hash.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

A="${CLICKHOUSE_DATABASE}.\`inner.t\`"
B="\`${CLICKHOUSE_DATABASE}.inner\`.t"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS \`${CLICKHOUSE_DATABASE}.inner\`;
CREATE DATABASE \`${CLICKHOUSE_DATABASE}.inner\`;
CREATE TABLE $A (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO $A VALUES (1);
CREATE TABLE $B (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO $B VALUES (2);

SELECT * FROM (SELECT 'view', x FROM view(SELECT x FROM $A) UNION ALL SELECT 'view', x FROM view(SELECT x FROM $B)) ORDER BY x;

SELECT 'query cache', x FROM $A SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0;
SELECT 'query cache', x FROM $B SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0;

DROP DATABASE \`${CLICKHOUSE_DATABASE}.inner\`;
"
