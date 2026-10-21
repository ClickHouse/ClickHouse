#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column named like a subcolumn that an aggregate projection or a TTL expression reads (`a.size0` of an `Array`
# column `a`) can no longer be added by `ALTER TABLE ... ADD COLUMN`, but a table where an older version added one can
# still exist. Its projection parts hold states computed from the sizes of `a`, so they are not used for the parts that
# miss the column, and they are rebuilt on merge. Its TTL infos are computed from the sizes of `a`, so the parts that
# miss the column keep reading them for `a.size0`. The upgrade is simulated by editing the metadata of the table offline.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

clickhouse_local()
{
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1"
}

clickhouse_local "
    CREATE DATABASE d ENGINE = Atomic;
    CREATE TABLE d.t_projection (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO d.t_projection VALUES ([0]), ([1, 2]);
    ALTER TABLE d.t_projection ADD PROJECTION p (SELECT sum(\`a.size0\`));
    ALTER TABLE d.t_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;
    CREATE TABLE d.t_ttl (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple() TTL addDays(toDate('2000-01-01'), 36500 * \`a.size0\`);
    INSERT INTO d.t_ttl VALUES ([1]);"

# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)\(,\?\)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 7\1/' "${WORKING_FOLDER}/metadata/d/t_projection.sql"
# shellcheck disable=SC2016
sed -i 's/^    `a` Array(UInt64)\(,\?\)$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 0\1/' "${WORKING_FOLDER}/metadata/d/t_ttl.sql"

clickhouse_local "
    SELECT 'projection';
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 0;
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 1;
    INSERT INTO d.t_projection VALUES ([5], 1);
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 0;
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 1;
    OPTIMIZE TABLE d.t_projection FINAL;
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 0;
    SELECT sum(\`a.size0\`) FROM d.t_projection SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

    SELECT 'ttl';
    SELECT a, \`a.size0\` FROM d.t_ttl;
    INSERT INTO d.t_ttl VALUES ([2], 0);
    OPTIMIZE TABLE d.t_ttl FINAL;
    SELECT a, \`a.size0\` FROM d.t_ttl;"

rm -rf "${WORKING_FOLDER}"
