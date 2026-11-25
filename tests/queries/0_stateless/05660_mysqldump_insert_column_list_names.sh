#!/usr/bin/env bash
# The column list of a MySQL dump `INSERT` can use double-quoted names (mysqldump --compatible=ansi), span several lines,
# differ in case from `CREATE TABLE` (also for non-ASCII names) or escape a quote by doubling it. The values must still be
# read into the right columns, a name not in `CREATE TABLE` must follow input_format_skip_unknown_fields, and distinct
# names that are not valid UTF-8 must not be matched to each other. A name that matches the given structure exactly
# must keep its spelling.

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

printf 'CREATE TABLE t1 (id int NOT NULL, `\xc3\x84` int DEFAULT NULL);\nINSERT INTO t1 (`\xc3\xa4`, id) VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id"

printf 'CREATE TABLE t1 (id int NOT NULL, b int DEFAULT NULL);\nINSERT INTO t1 (id, c) VALUES (1,100),(2,200);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) SETTINGS input_format_skip_unknown_fields = 0" 2>&1 | grep -q -F "INCORRECT_DATA" && echo "unknown field rejected"

printf 'CREATE TABLE "t1" ("a""b" int NOT NULL, "c" int DEFAULT NULL);\nINSERT INTO "t1" ("c", "a""b") VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY ALL"

printf 'CREATE TABLE `t1` (`a``b` int NOT NULL, c int DEFAULT NULL);\nINSERT INTO `t1` (c, `a``b`) VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY ALL"

printf 'INSERT INTO "t1" ("a""b", "c") VALUES (1,100),(2,200);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}', MySQLDump)"

printf 'CREATE TABLE "t0" ("x" int);\nINSERT INTO "t0" VALUES (5);\nCREATE TABLE "t1" ("id" int NOT NULL, "b" int DEFAULT NULL);\nINSERT INTO "t1" ("b", "id") VALUES (100,1),(200,2);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump) ORDER BY id SETTINGS input_format_mysql_dump_table_name = 't1'"

printf 'CREATE TABLE t1 (`\xe4` int, `\xf6` int);\nINSERT INTO t1 (`\xf6`) VALUES (100);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump)"

printf 'CREATE TABLE t1 (`\xe4` int, `\xf6` int);\nINSERT INTO t1 (`\xf6`, `\xe4`) VALUES (100,1);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump)"

printf 'CREATE TABLE t1 (id int NOT NULL, b int NOT NULL);\nINSERT INTO t1 (B, ID) VALUES (100,1);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump, 'ID Int32, B Int32')"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump, 'ID Int32, B Int32') SETTINGS input_format_skip_unknown_fields = 0"

printf 'CREATE TABLE t1 (id int, b int);\nINSERT INTO t1 (b\f, id\v) VALUES (100,1);\n' > "${DATA_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', MySQLDump)"
