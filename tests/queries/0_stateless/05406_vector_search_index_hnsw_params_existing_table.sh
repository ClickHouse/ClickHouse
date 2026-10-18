#!/usr/bin/env bash
# Tags: no-fasttest

# A table created before the HNSW parameters were bounded must stay alterable,
# while a new declaration above the bounds is still rejected.
# ATTACH TABLE with an inline definition is only accepted by the Ordinary database engine,
# whose creation emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORD="${CLICKHOUSE_DATABASE}_ord"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${ORD}"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"

$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD}.tab (id UInt64, vec Array(Float32), vec_b Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 1000, 128)) ENGINE = MergeTree ORDER BY id"

echo 'an unrelated ALTER is allowed'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab ADD COLUMN x UInt8" && echo ok

echo 'a new index with the same parameters is rejected'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab ADD INDEX idx2 vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 1000, 128)" 2>&1 | grep -o -m1 'INCORRECT_DATA'

echo 'redeclaring the index under its name is rejected'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab DROP INDEX idx, ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 2994230, 400)" 2>&1 | grep -o -m1 'INCORRECT_DATA'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab DROP INDEX idx, ADD INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 1000, 128)" 2>&1 | grep -o -m1 'INCORRECT_DATA'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab DROP INDEX idx, ADD INDEX idx vec_b TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 1000, 128)" 2>&1 | grep -o -m1 'INCORRECT_DATA'

echo 'changing the type of the indexed column is rejected'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab MODIFY COLUMN vec Array(Float64)" 2>&1 | grep -o -m1 'INCORRECT_DATA'

echo 'the expression of an index the table has is still validated'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${ORD}.tab_alias (x UInt64, a UInt64 ALIAS x, INDEX i a TYPE minmax) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab_alias MODIFY COLUMN a UInt64 ALIAS rand64()" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo 'renaming the indexed column is allowed'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab RENAME COLUMN vec TO vec2" && echo ok

echo 'the index can be dropped'
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${ORD}.tab DROP INDEX idx" && echo ok
$CLICKHOUSE_CLIENT -q "SELECT name, type FROM system.columns WHERE database = '${ORD}' AND table = 'tab' ORDER BY position"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${ORD} SYNC"
