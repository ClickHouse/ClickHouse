#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table where an older version added a column `a.size0` next to an `Array` column `a` while an aggregate projection
# reads `a.size0` (see `05410_column_shadowing_subcolumn_legacy_projection_and_ttl`). Its projection parts hold states
# computed from the sizes of `a`. An unrelated mutation that rewrites a `Compact` part writes the column `a.size0` from
# its default, so the projection is rebuilt instead of being carried over with the old states.
# The upgrade is simulated by editing the metadata of the table offline.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

clickhouse_local()
{
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1"
}

clickhouse_local "
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t (a Array(UInt64), z UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
    INSERT INTO d.t VALUES ([0], 0), ([1, 2], 0);
    ALTER TABLE d.t ADD PROJECTION p (SELECT sum(\`a.size0\`));
    ALTER TABLE d.t MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;"

# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)\(,\?\)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 7\1/' "${WORKING_FOLDER}/metadata/d/t.sql"

clickhouse_local "
    SELECT sum(\`a.size0\`) FROM d.t SETTINGS optimize_use_projections = 0;
    SELECT sum(\`a.size0\`) FROM d.t SETTINGS optimize_use_projections = 1;
    ALTER TABLE d.t UPDATE z = z + 1 WHERE 1 SETTINGS mutations_sync = 1;
    SELECT sum(\`a.size0\`) FROM d.t SETTINGS optimize_use_projections = 0;
    SELECT sum(\`a.size0\`) FROM d.t SETTINGS optimize_use_projections = 1;
    SELECT sum(\`a.size0\`) FROM d.t SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
    SELECT sum(z) FROM d.t;"

rm -rf "${WORKING_FOLDER}"
