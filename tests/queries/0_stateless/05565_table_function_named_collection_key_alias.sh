#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query alias with the name of a key must not replace the key in a `key = value` named collection override.
# https://github.com/ClickHouse/ClickHouse/issues/129895

nc="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP NAMED COLLECTION IF EXISTS \`${nc}\`;
CREATE NAMED COLLECTION \`${nc}\` AS host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP};
CREATE TABLE t (x UInt8) ENGINE = Memory;
INSERT INTO t VALUES (1), (2);
"

${CLICKHOUSE_CLIENT} --query "SELECT 'x' AS table, count() FROM remote(\`${nc}\`, database = currentDatabase(), table = 't')"
${CLICKHOUSE_CLIENT} --query "SELECT 'default' AS database, count() FROM remote(\`${nc}\`, database = currentDatabase(), table = 't')"
${CLICKHOUSE_CLIENT} --query "SELECT 1 AS database, count() FROM remote(\`${nc}\`, database = currentDatabase(), table = 't')"
${CLICKHOUSE_CLIENT} --query "WITH 'x' AS table SELECT count() FROM remote(\`${nc}\`, database = currentDatabase(), table = 't')"
${CLICKHOUSE_CLIENT} --query "SELECT 'default' AS database, (SELECT count() FROM remote(\`${nc}\`, database = currentDatabase(), table = 't'))"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION \`${nc}\`"
