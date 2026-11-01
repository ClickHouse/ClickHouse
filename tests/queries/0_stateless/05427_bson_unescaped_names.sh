#!/usr/bin/env bash
# `BSONEachRow` output writes element names (column names, `Tuple` element names, `Map` keys) as they are. It used to
# JSON-escape them (`path/to` became `path\/to`), so a reload lost the columns and changed the keys.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT 1::UInt32 AS id, 'x' AS \`path/to\`, 9.5 AS \`price\"usd\`, map('url', 'http://a/b', 'dir/name', 'v', 'q\"k', 'w') AS attrs,
        tuple(1, 2)::Tuple(\`a/b\` UInt8, \`e\"f\` UInt8) AS t
    INTO OUTFILE '${DATA_FILE}.bson' TRUNCATE FORMAT BSONEachRow" < /dev/null

echo "backslashes in the file: $(tr -cd '\\' < "${DATA_FILE}.bson" | wc -c)"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}.bson', BSONEachRow); SELECT * FROM file('${DATA_FILE}.bson', BSONEachRow)" < /dev/null

echo "--- a zero byte in a name is written as \\u0000"
${CLICKHOUSE_LOCAL} --query "SELECT map('a' || char(0) || 'b', 1) AS m FORMAT BSONEachRow" < /dev/null \
    | ${CLICKHOUSE_LOCAL} --input-format BSONEachRow --table test --structure "m Map(String, UInt32)" --query "SELECT * FROM test"
