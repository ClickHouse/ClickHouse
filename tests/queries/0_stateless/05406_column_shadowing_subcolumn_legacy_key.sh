#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column named like a subcolumn that the sorting or partition key reads (`a.size0` of an `Array` column `a`)
# can no longer be added by `ALTER TABLE ... ADD COLUMN`, but a table where an older version added one can still
# exist. Its old parts are indexed and partitioned by the sizes of `a`, so they keep reading them for `a.size0`,
# and pruning agrees with the values read. The upgrade is simulated by editing the metadata of the table offline.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

clickhouse_local()
{
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1"
}

clickhouse_local "
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t_order_by (a Array(UInt64)) ENGINE = MergeTree ORDER BY \`a.size0\` SETTINGS index_granularity = 1;
    INSERT INTO d.t_order_by VALUES ([0]), ([1, 2]);
    CREATE TABLE d.t_partition_by (a Array(UInt64)) ENGINE = MergeTree PARTITION BY \`a.size0\` ORDER BY tuple();
    INSERT INTO d.t_partition_by VALUES ([0]), ([1, 2]);
    CREATE TABLE d.t_sorting_suffix (id UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY (id, \`a.size0\`) PRIMARY KEY id;
    INSERT INTO d.t_sorting_suffix VALUES (1, [0]), (1, [1, 2]), (1, [1, 2, 3]);"

# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 7/' \
    "${WORKING_FOLDER}/metadata/d/t_order_by.sql" "${WORKING_FOLDER}/metadata/d/t_partition_by.sql"
# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 10 - length(a)/' \
    "${WORKING_FOLDER}/metadata/d/t_sorting_suffix.sql"

clickhouse_local "
    SELECT 'order by';
    INSERT INTO d.t_order_by VALUES ([5], 7);
    SELECT a, \`a.size0\` FROM d.t_order_by ORDER BY a;
    SELECT count() FROM d.t_order_by WHERE \`a.size0\` = 2;
    SELECT countIf(\`a.size0\` = 2) FROM d.t_order_by;
    SELECT count() FROM d.t_order_by WHERE \`a.size0\` = 7;
    SELECT countIf(\`a.size0\` = 7) FROM d.t_order_by;
    OPTIMIZE TABLE d.t_order_by FINAL;
    SELECT a, \`a.size0\` FROM d.t_order_by ORDER BY \`a.size0\` SETTINGS optimize_read_in_order = 1;
    SELECT count() FROM d.t_order_by WHERE \`a.size0\` = 2;

    SELECT 'partition by';
    SELECT a, \`a.size0\` FROM d.t_partition_by ORDER BY a;
    SELECT count() FROM d.t_partition_by WHERE \`a.size0\` = 2;
    SELECT countIf(\`a.size0\` = 2) FROM d.t_partition_by;

    SELECT 'sorting key suffix';
    SELECT id, \`a.size0\` FROM d.t_sorting_suffix ORDER BY id, \`a.size0\` SETTINGS optimize_read_in_order = 1;
    OPTIMIZE TABLE d.t_sorting_suffix FINAL;
    SELECT id, \`a.size0\` FROM d.t_sorting_suffix ORDER BY id, \`a.size0\` SETTINGS optimize_read_in_order = 1;"

rm -rf "${WORKING_FOLDER}"
