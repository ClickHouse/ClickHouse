#!/usr/bin/env bash
# An INSERT that directly follows CREATE TABLE in a MySQL dump must be read: the CREATE query is consumed up to its
# ';' while its structure is extracted, so no further query may be skipped after it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f "${DATA_FILE}"' EXIT

printf 'CREATE TABLE `t1` (`x` int DEFAULT NULL) ENGINE=InnoDB;\nINSERT INTO `t1` VALUES (1),(2);\nINSERT INTO `t1` VALUES (3);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT x FROM file('${DATA_FILE}', MySQLDump) ORDER BY x"

# The only INSERT of the dump.
printf "CREATE TABLE \`t1\` (\`x\` int DEFAULT NULL, \`s\` varchar(10)) ENGINE=InnoDB;\nINSERT INTO \`t1\` VALUES (1,'a');\n" > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_FILE}', MySQLDump);
    SELECT * FROM file('${DATA_FILE}', MySQLDump);"
