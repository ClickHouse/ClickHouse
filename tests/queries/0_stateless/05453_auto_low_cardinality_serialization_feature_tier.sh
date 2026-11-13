#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `max_uniq_number_for_low_cardinality` enables an experimental storage encoding, so it is refused
# when `allow_feature_tier` disallows experimental settings, both in `CREATE TABLE` and in `ALTER TABLE`.
${CLICKHOUSE_LOCAL} -n -q "
    SELECT tier FROM system.merge_tree_settings WHERE name = 'max_uniq_number_for_low_cardinality';
    CREATE TABLE t (s String) ENGINE = MergeTree ORDER BY () SETTINGS max_uniq_number_for_low_cardinality = 100; -- { serverError READONLY }
    CREATE TABLE t2 (s String) ENGINE = MergeTree ORDER BY ();
    ALTER TABLE t2 MODIFY SETTING max_uniq_number_for_low_cardinality = 100; -- { serverError READONLY }
    SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't%' ORDER BY name;
" -- --allow_feature_tier=1

# With experimental settings allowed, both succeed.
${CLICKHOUSE_LOCAL} -n -q "
    CREATE TABLE t (s String) ENGINE = MergeTree ORDER BY () SETTINGS max_uniq_number_for_low_cardinality = 100;
    CREATE TABLE t2 (s String) ENGINE = MergeTree ORDER BY ();
    ALTER TABLE t2 MODIFY SETTING max_uniq_number_for_low_cardinality = 100;
    SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't%' ORDER BY name;
" -- --allow_feature_tier=0
