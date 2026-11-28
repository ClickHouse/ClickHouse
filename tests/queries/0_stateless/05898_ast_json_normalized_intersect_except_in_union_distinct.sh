#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A normalized `SelectWithUnionQuery` can hold a binary `INTERSECT` / `EXCEPT` (the form the query tree
# writes), and the AST JSON reader accepts it. The visitors skip such a node, but when it is an operand
# of a `UNION DISTINCT` that is not normalized yet, the normalization of the enclosing union used to
# flatten it without looking at its mode: `SELECT 1 UNION DISTINCT (SELECT 2 EXCEPT ALL SELECT 2)`
# became `SELECT 1 UNION DISTINCT SELECT 2 UNION DISTINCT SELECT 2` and returned the row `2` as well.
# SQL text never produces such a tree, so `dialect = clickhouse_json` is the only way to reach it,
# and the query has to be executed: formatting does not run the visitors.

# $1 is the query, $2 the mode of its only `INTERSECT` / `EXCEPT`. The node holding that operation is
# rewritten to its normalized form, the enclosing union stays as the parser produced it.
run_json_ast()
{
    local json
    json=$(${CLICKHOUSE_LOCAL} -q "
        SELECT replace(parseQueryToJSON('$1'),
            '\"union_mode\":\"UNION_DEFAULT\",\"is_normalized\":false,\"list_of_modes\":[\"$2\"],',
            '\"union_mode\":\"$2\",\"is_normalized\":true,')
        FORMAT TSVRaw")

    # The rewrite must have happened, otherwise the test would execute the plain query and pass for the wrong reason.
    grep -c -F "\"union_mode\":\"$2\",\"is_normalized\":true" <<< "$json"

    echo "$1"
    ${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$json" | sort
}

# The set operation removes or keeps its own rows only: `2` is not in the result.
run_json_ast "SELECT 1 UNION DISTINCT (SELECT 2 EXCEPT ALL SELECT 2)" EXCEPT_ALL
run_json_ast "SELECT 1 UNION DISTINCT (SELECT 2 EXCEPT DISTINCT SELECT 2)" EXCEPT_DISTINCT
run_json_ast "SELECT 1 UNION DISTINCT (SELECT 2 INTERSECT ALL SELECT 3)" INTERSECT_ALL
run_json_ast "SELECT 1 UNION DISTINCT (SELECT 2 INTERSECT DISTINCT SELECT 3)" INTERSECT_DISTINCT

# The same for an operand that is not the last one of the `UNION DISTINCT` list.
run_json_ast "(SELECT 2 EXCEPT ALL SELECT 2) UNION DISTINCT SELECT 1" EXCEPT_ALL
run_json_ast "SELECT 1 UNION ALL SELECT 5 UNION DISTINCT (SELECT 2 EXCEPT ALL SELECT 2) UNION DISTINCT SELECT 1" EXCEPT_ALL

# A non-empty result of the set operation takes part in the union as a whole.
run_json_ast "SELECT 1 UNION DISTINCT (SELECT 2 INTERSECT ALL SELECT 2) UNION ALL SELECT 4" INTERSECT_ALL

# `UNION ALL` did not flatten such an operand: unchanged.
run_json_ast "SELECT 1 UNION ALL (SELECT 2 EXCEPT ALL SELECT 2)" EXCEPT_ALL
