#!/usr/bin/env bash
# A `SET` column of a MySQL dump is read as a string of its members, as the `mysql` table engine reads it: `SET` is
# also an alias of `UInt64`, which can't parse the dumped values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f "${DATA_FILE}"' EXIT

printf "CREATE TABLE \`t1\` (\`s\` set('a','b','c') DEFAULT NULL, \`n\` set('x') NOT NULL) ENGINE=InnoDB;\n/*!40101 SET character_set_client = @saved_cs_client */;\nINSERT INTO \`t1\` VALUES ('a,b','x'),(NULL,''),('c','x');\n" > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_FILE}', MySQLDump);
    SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY s;"
