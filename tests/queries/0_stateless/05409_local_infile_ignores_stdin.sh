#!/usr/bin/env bash
# `INSERT ... FROM INFILE` inserts only the file. It used to insert the data piped to stdin as well, and to wait for
# an open stdin pipe to close. `02270_stdin_with_query_or_infile_data` checks the same for `clickhouse-client`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT
printf 'a\nb\n' > "${DATA_FILE}.csv"

echo "--- stdin with data"
echo 'FROM_STDIN' | ${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (s String) ENGINE = Memory;
    INSERT INTO t FROM INFILE '${DATA_FILE}.csv' FORMAT CSV;
    SELECT * FROM t ORDER BY s"

echo "--- single query"
echo 'FROM_STDIN' | ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}.out.csv', CSV, 's String') FROM INFILE '${DATA_FILE}.csv' FORMAT CSV"
cat "${DATA_FILE}.out.csv"

echo "--- queries file"
echo "CREATE TABLE t (s String) ENGINE = Memory; INSERT INTO t FROM INFILE '${DATA_FILE}.csv' FORMAT CSV; SELECT * FROM t ORDER BY s;" > "${DATA_FILE}.sql"
echo 'FROM_STDIN' | ${CLICKHOUSE_LOCAL} --queries-file "${DATA_FILE}.sql"

echo "--- stdin pipe that stays open"
# The writer keeps the pipe open for a minute; the query must not wait for it.
exec {stdin_fd}< <(sleep 60)
writer_pid=$!
timeout 30 ${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (s String) ENGINE = Memory;
    INSERT INTO t FROM INFILE '${DATA_FILE}.csv' FORMAT CSV;
    SELECT * FROM t ORDER BY s" <&"${stdin_fd}"
echo "exit code $?"
exec {stdin_fd}<&-
kill "${writer_pid}" 2>/dev/null
