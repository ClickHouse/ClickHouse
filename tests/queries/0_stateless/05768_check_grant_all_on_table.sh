#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user=user_${CLICKHOUSE_TEST_UNIQUE_NAME}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user; DROP TABLE IF EXISTS ${db}.t;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.t (x UInt8) ENGINE = Memory; CREATE USER $user; GRANT ALL ON ${db}.t TO $user;"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT SELECT ON ${db}.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT ALL ON ${db}.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT ALL(x) ON ${db}.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT ALL ON ${db}.*"

${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${db}.t FROM $user;"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$user" --data-binary "CHECK GRANT ALL ON ${db}.t"

${CLICKHOUSE_CLIENT} --query "DROP USER $user; DROP TABLE ${db}.t;"
