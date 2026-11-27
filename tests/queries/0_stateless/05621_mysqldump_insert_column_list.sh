#!/usr/bin/env bash
# The values of a MySQL dump are read by the column list of its `INSERT`, not by the columns of `CREATE TABLE`:
# mysqldump leaves generated columns out of the `INSERT` and lists the dumped columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f "${DATA_FILE}"' EXIT

printf "CREATE TABLE \`t1\` (\`id\` int NOT NULL, \`g\` int GENERATED ALWAYS AS ((\`id\` * 10)) VIRTUAL, \`b\` int DEFAULT NULL) ENGINE=InnoDB;\nINSERT INTO \`t1\` (\`id\`, \`b\`) VALUES (1,100),(2,200);\n" > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_FILE}', MySQLDump);
    SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id;
    SELECT * FROM file('${DATA_FILE}', MySQLDump, 'id Int32, b Nullable(Int32)') ORDER BY id;"
