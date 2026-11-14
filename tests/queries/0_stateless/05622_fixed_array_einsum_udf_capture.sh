#!/usr/bin/env bash

# The parameters generated for the lambda form of `einsum` do not capture the free names in the SQL user defined functions
# called in the expression, also transitively.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INNER="${CLICKHOUSE_DATABASE}_einsum_capture"
OUTER="${CLICKHOUSE_DATABASE}_einsum_capture_outer"

$CLICKHOUSE_CLIENT --enable_analyzer 1 -q "
  CREATE FUNCTION ${INNER} AS value -> value + x1;
  CREATE FUNCTION ${OUTER} AS value -> ${INNER}(value) * 2;

  SELECT einsum(i -> ${INNER}(a[i])) FROM (SELECT [1, 2] AS a, 10 AS x1);
  SELECT arrayMap(v -> ${INNER}(v), a) FROM (SELECT [1, 2] AS a, 10 AS x1);
  SELECT einsum(i -> ${OUTER}(a[i])) FROM (SELECT [1, 2] AS a, 10 AS x1);
  SELECT arrayMap(v -> ${OUTER}(v), a) FROM (SELECT [1, 2] AS a, 10 AS x1);

  DROP FUNCTION ${OUTER};
  DROP FUNCTION ${INNER};
"
