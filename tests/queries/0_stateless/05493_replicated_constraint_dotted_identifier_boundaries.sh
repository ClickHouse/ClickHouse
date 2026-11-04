#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments.
# Tag no-shared-merge-tree: same reason.

# Stored definitions are compared as ASTs. `db.`inner.t`` and ``db.inner`.t`` have the same
# dot-joined name but refer to different tables, so they must not compare equal: a replica with the
# other form must not join, and replacing one form with the other must take effect.

DB="${CLICKHOUSE_DATABASE}"
INNER_DB="${CLICKHOUSE_DATABASE}.inner"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS \`${INNER_DB}\`;
CREATE DATABASE \`${INNER_DB}\`;
CREATE TABLE \`${DB}\`.\`inner.t\` (x UInt8) ENGINE = Memory;
CREATE TABLE \`${INNER_DB}\`.t (x UInt8) ENGINE = Memory;
INSERT INTO \`${DB}\`.\`inner.t\` VALUES (1);
INSERT INTO \`${INNER_DB}\`.t VALUES (2);

CREATE TABLE \`${DB}\`.r1 (a UInt8, CONSTRAINT c CHECK a IN (SELECT x FROM \`${DB}\`.\`inner.t\`))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/dotted_boundaries', 'r1') ORDER BY a;
"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE \`${DB}\`.r2 (a UInt8, CONSTRAINT c CHECK a IN (SELECT x FROM \`${INNER_DB}\`.t))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/dotted_boundaries', 'r2') ORDER BY a;
" 2>&1 | grep -o -m1 'METADATA_MISMATCH'

$CLICKHOUSE_CLIENT -m -q "
INSERT INTO \`${DB}\`.r1 VALUES (1);
ALTER TABLE \`${DB}\`.r1 DROP CONSTRAINT c, ADD CONSTRAINT c CHECK a IN (SELECT x FROM \`${INNER_DB}\`.t);
INSERT INTO \`${DB}\`.r1 VALUES (2);
SELECT a FROM \`${DB}\`.r1 ORDER BY a;
"
$CLICKHOUSE_CLIENT -q "INSERT INTO \`${DB}\`.r1 VALUES (1)" 2>&1 | grep -o -m1 'VIOLATED_CONSTRAINT'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND name = 'r1' AND create_table_query LIKE '%\`${INNER_DB}\`.t%'"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE \`${DB}\`.r1 SYNC;
DROP DATABASE \`${INNER_DB}\`;
"
