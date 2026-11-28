#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, zookeeper, no-fasttest
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
# no-shared-merge-tree: the table engine is replaced, and it keeps parts elsewhere.
# no-fasttest: replicated tables are slow to create and drop in the fast test, which limits a new test to 15 seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

engine=ReplicatedMergeTree
part=1
# shellcheck source=./lazy_load_tables_merge_tree_operations.lib
. "$CUR_DIR"/lazy_load_tables_merge_tree_operations.lib
