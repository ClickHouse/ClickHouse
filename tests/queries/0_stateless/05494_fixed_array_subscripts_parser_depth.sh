#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The expansion of the subscripts `arr[]` and `arr[i, j]` runs before the depth of the query is checked,
# so it has to respect the depth limit itself instead of overflowing the stack on a deep expression.

python3 -c "print('SELECT ' + '1+' * 60000 + '[1][]')" > "${CLICKHOUSE_TMP}/deep_subscripts.sql"
${CLICKHOUSE_CLIENT} --queries-file "${CLICKHOUSE_TMP}/deep_subscripts.sql" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION\|TOO_DEEP_AST'

python3 -c "print('SELECT [1]' + '[]' * 60000 + '[1]')" > "${CLICKHOUSE_TMP}/deep_subscripts.sql"
${CLICKHOUSE_CLIENT} --queries-file "${CLICKHOUSE_TMP}/deep_subscripts.sql" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION\|TOO_DEEP_AST'

${CLICKHOUSE_CLIENT} --query "SELECT [[1, 2], [3, 4]][][2], [[1, 2], [3, 4]][2, 1]"

rm -f "${CLICKHOUSE_TMP}/deep_subscripts.sql"
