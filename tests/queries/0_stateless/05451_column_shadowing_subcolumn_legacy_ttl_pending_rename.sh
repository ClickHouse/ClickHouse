#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In a table where an older version added a column named like a subcolumn that a TTL expression reads (`a.size0` of an
# `Array` column `a`), the parts that miss the column keep reading the sizes of `a` for it. They must keep doing so after
# the column is renamed, while the rename is not yet applied to them: the TTL expression of the current metadata reads
# the new name. The upgrade is simulated by editing the metadata of the table offline.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

clickhouse_local()
{
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1"
}

clickhouse_local "
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple() TTL addDays(toDate('2000-01-01'), 36500 * \`a.size0\`);
    INSERT INTO d.t VALUES ([1]);"

# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)\(,\?\)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 0\1/' "${WORKING_FOLDER}/metadata/d/t.sql"

clickhouse_local "
    SYSTEM STOP MERGES d.t;
    SELECT a, \`a.size0\` FROM d.t;
    ALTER TABLE d.t RENAME COLUMN \`a.size0\` TO \`a.sz\` SETTINGS alter_sync = 0, mutations_sync = 0;
    SELECT a, \`a.sz\` FROM d.t;"

rm -rf "${WORKING_FOLDER}"
