#!/usr/bin/env bash
# Tags: no-fasttest
# The predicates must reach the server limit of 500000 nodes, which takes longer than Fast test allows for a new test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

# A mutate task applies a batch of mutations and analyzes their combined predicate, which must fit into
# `max_expanded_ast_elements` (500000 by default) in query tree nodes. Each predicate here is 1400 constant terms:
# 9806 AST nodes, but 15404 query tree nodes, because a folded constant keeps its source expression.
# 45 such mutations fit into the limit by AST size, but not by query tree size, so the batch must be built by
# query tree size, otherwise every attempt fails with "Query tree is too big".

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_mutations_tree_size;
    CREATE TABLE t_mutations_tree_size (a UInt32) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_mutations_tree_size SELECT number FROM numbers(100);
    SYSTEM STOP MERGES t_mutations_tree_size;
"

# The predicates are constant and false, so each task is quick and deletes nothing.
$CLICKHOUSE_CLIENT -q "
    SELECT 'ALTER TABLE t_mutations_tree_size DELETE WHERE '
        || arrayStringConcat(arrayMap(i -> '(' || toString(m * 10000 + i) || ' + 1 = 0)', range(1400)), ' OR ')
        || ';'
    FROM (SELECT number AS m FROM numbers(45))
    FORMAT TSVRaw
" | $CLICKHOUSE_CLIENT --mutations_sync 0

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_mutations_tree_size"
wait_for_all_mutations "t_mutations_tree_size"

$CLICKHOUSE_CLIENT -q "
    SELECT countIf(is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutations_tree_size';
    SELECT count() FROM t_mutations_tree_size;
    DROP TABLE t_mutations_tree_size;
"
