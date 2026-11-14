#!/usr/bin/env bash
# The column list of a MySQL dump `INSERT` can use double-quoted names (mysqldump --compatible=ansi), span several lines
# or differ in case from `CREATE TABLE`. The values must still be read into the right columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f "${DATA_FILE}"' EXIT

printf 'CREATE TABLE "t1" ("id" int NOT NULL, "b" int DEFAULT NULL);\nINSERT INTO "t1" ("id", "b") VALUES (1,100),(2,200);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id"

printf 'CREATE TABLE t1 (id int NOT NULL, b int DEFAULT NULL);\nINSERT INTO t1 (\n  b,\n  id\n) VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id"

printf 'CREATE TABLE t1 (id int NOT NULL, b int DEFAULT NULL);\nINSERT INTO t1 (B, ID) VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id"

printf 'CREATE TABLE t1 (id int NOT NULL, b int DEFAULT NULL);\nINSERT INTO t1 (id, c) VALUES (1,100),(2,200);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id"

printf 'INSERT INTO "t1" ("id", "b") VALUES (1,100),(2,200);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}', MySQLDump)"
