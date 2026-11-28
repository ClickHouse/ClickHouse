#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Checks the output of the keeper-client commands that traverse a subtree
# (`find_super_nodes`, `find_big_family`, `get_all_children_number`) on a tree with
# a deep chain, a node with more children than the traversal keeps requests in flight (50),
# and subtrees of equal size.

path="/test-keeper-client-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null || true

# $path/chain/c1/.../c100: a chain of depth 100.
# $path/wide: 60 children, the first of them has 5 children of its own, which are traversed while
#             the other children of $path/wide wait for a free slot.
# $path/fam/{a,b,c,d}: 4 subtrees of 4 nodes each.
queries="create '$path' 'x'; create '$path/chain' 'x'"
node="$path/chain"
for i in $(seq 1 100); do
    node="$node/c$i"
    queries="$queries; create '$node' 'x'"
done
queries="$queries; create '$path/wide' 'x'"
for i in $(seq -w 0 59); do
    queries="$queries; create '$path/wide/w$i' 'x'"
done
for i in 1 2 3 4 5; do
    queries="$queries; create '$path/wide/w00/x$i' 'x'"
done
queries="$queries; create '$path/fam' 'x'"
for s in a b c d; do
    queries="$queries; create '$path/fam/$s' 'x'"
    for i in 1 2 3; do
        queries="$queries; create '$path/fam/$s/$i' 'x'"
    done
done
$CLICKHOUSE_KEEPER_CLIENT -q "$queries"

echo 'find_super_nodes'
# A node with exactly the threshold number of children is reported, children of super nodes are not traversed.
$CLICKHOUSE_KEEPER_CLIENT -q "find_super_nodes 4 '$path'" | sort
# The start node itself can be reported.
$CLICKHOUSE_KEEPER_CLIENT -q "find_super_nodes 3 '$path/fam/a'"
$CLICKHOUSE_KEEPER_CLIENT -q "find_super_nodes 1 '$path/nonexistent'"

echo 'find_big_family'
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path' 5"
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path/wide' 3"
# Ties are ordered by path in descending order, the limit cuts through them.
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path/fam' 3"
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path/fam' 100"
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path/fam' 0"
$CLICKHOUSE_KEEPER_CLIENT -q "find_big_family '$path/nonexistent' 3"

echo 'get_all_children_number'
$CLICKHOUSE_KEEPER_CLIENT -q "get_all_children_number '$path'"
$CLICKHOUSE_KEEPER_CLIENT -q "get_all_children_number '$path/wide'"
$CLICKHOUSE_KEEPER_CLIENT -q "get_all_children_number '$path/wide/w01'"
$CLICKHOUSE_KEEPER_CLIENT -q "get_all_children_number '$path/nonexistent'" 2>&1 | grep -o -m1 "node doesn't exist"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"
