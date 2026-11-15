#!/usr/bin/env bash
# The layer filter of `PartsSplitter` refers to the key columns by name and is composed with the table's
# own key expression. An aliased key, whose `sample_block` names differ from the alias-free key column names,
# used to break it.
#
# Such a key can no longer be written through SQL, but a table that an older server stored with it still loads,
# so the stored metadata of a table created without the alias is edited into that form.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t_pk_alias (k UInt64, v UInt64) ENGINE = ReplacingMergeTree PRIMARY KEY k ORDER BY k SETTINGS index_granularity = 1;
"

metadata_file=$(grep -rl 'ENGINE = ReplacingMergeTree' "${WORKING_DIR}" --include='t_pk_alias.sql')
sed -i 's/^PRIMARY KEY k$/PRIMARY KEY (k AS a)/' "${metadata_file}"
# Without this the query below would run against an unaliased key, i.e. assert nothing.
grep -c -F 'PRIMARY KEY (k AS a)' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SYSTEM STOP MERGES db.t_pk_alias;
INSERT INTO db.t_pk_alias VALUES (1, 1), (3, 1);
INSERT INTO db.t_pk_alias VALUES (2, 2), (4, 2);
INSERT INTO db.t_pk_alias VALUES (2, 3), (5, 3);

SELECT k, v FROM db.t_pk_alias FINAL ORDER BY k
SETTINGS max_threads = 4, do_not_merge_across_partitions_select_final = 0,
         split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
"

rm -rf "${WORKING_DIR}"
