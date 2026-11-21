#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Many matchers replace the GROUP BY key with itself. The walk over the replacements that looks for
# positional arguments must stay linear in their number, otherwise this query takes much longer and much more memory.

MATCHERS=$(yes '* REPLACE (c AS c)' | head -n 3000 | paste -sd, -)

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --query "
    SELECT count() FROM (
        SELECT ${MATCHERS}
        FROM (SELECT number AS c FROM numbers(1)) AS t
        GROUP BY c WITH ROLLUP
        SETTINGS group_by_use_nulls = 1)"
