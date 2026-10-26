#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names) as column qualifiers.

db=$CLICKHOUSE_DATABASE

# An explicit alias is tried before the longer table name: for the table `a.b` aliased `a`, `a.b.c` is the subcolumn
# `b.c` qualified with the alias (as before hierarchical names), not the column `c` qualified with the table name.
$CLICKHOUSE_CLIENT -nq "
CREATE TABLE \`a.b\` (c UInt8, b Tuple(c UInt8)) ENGINE = Memory;
INSERT INTO \`a.b\` VALUES (1, (2));
SELECT a.b.c FROM \`a.b\` AS a;
SELECT a.b.c FROM \`a.b\` AS a SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT a.c FROM \`a.b\` AS a;
SELECT a.b.c FROM \`a.b\`;
DROP TABLE \`a.b\`;
"

# A quoted part of a qualifier may cross the boundary between the database and the table: \`db.ns\`.\`t\` names the
# table \`ns.t\` of the database \`db\` (there is no database \`db.ns\`), in the source and in its column qualifier alike.
$CLICKHOUSE_CLIENT -nq "
CREATE TABLE \`ns.t\` (id UInt8) ENGINE = Memory;
INSERT INTO \`ns.t\` VALUES (3);
SELECT \`${db}.ns\`.t.id FROM \`${db}.ns\`.t;
SELECT ${db}.ns.t.id FROM ${db}.ns.t;
SELECT ${db}.\`ns.t\`.id FROM \`${db}.ns\`.t;
SELECT \`${db}.ns\`.t.* FROM \`${db}.ns\`.t;
DROP TABLE \`ns.t\`;
"
