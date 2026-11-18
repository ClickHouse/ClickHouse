#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: needs S3.
# no-replicated-database: with `access_control_improvements.enable_read_write_grants` disabled (the
# default) a replicated access storage round-trips the users through serialized SQL, which rewrites
# `READ ON S3` and `WRITE ON S3` into the old-style whole-source `S3` grant.

# A table of an `S3` database is resolved in read mode, which requires `READ ON S3`;
# writing to it requires `WRITE ON S3` as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READER="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
WRITER="w_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READER_WRITER="rw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
FILTERED_WRITER="fw_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="${DB}.\`f.csv\`"

${CLICKHOUSE_CLIENT} -q "
INSERT INTO FUNCTION s3('${PREFIX}f.csv', 'test', 'testtest', 'CSV', 'c1 UInt64') SELECT 3 SETTINGS s3_truncate_on_insert = 1;
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = S3('${PREFIX}', 'test', 'testtest');
"

for user in "${READER}" "${WRITER}" "${READER_WRITER}" "${FILTERED_WRITER}"; do
    ${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT, INSERT, TRUNCATE, SHOW COLUMNS ON ${DB}.* TO ${user};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
    "
done

${CLICKHOUSE_CLIENT} -q "
GRANT READ ON S3 TO ${READER};
GRANT WRITE ON S3 TO ${WRITER};
GRANT READ, WRITE ON S3 TO ${READER_WRITER};
GRANT READ ON S3 TO ${FILTERED_WRITER};
GRANT WRITE ON S3('${PREFIX}f.csv') TO ${FILTERED_WRITER};
"

echo '--- WRITE ON S3 only'
${CLICKHOUSE_CLIENT} --user "${WRITER}" -q "SELECT * FROM ${TABLE}" 2>&1 | grep -o -m1 'grant READ ON S3'
${CLICKHOUSE_CLIENT} --user "${WRITER}" -q "DESCRIBE TABLE ${TABLE}" 2>&1 | grep -o -m1 'grant READ ON S3'
${CLICKHOUSE_CLIENT} --user "${WRITER}" -q "INSERT INTO ${TABLE} SETTINGS s3_truncate_on_insert = 1 VALUES (4)" 2>&1 | grep -o -m1 'grant READ ON S3'

echo '--- READ ON S3 only'
${CLICKHOUSE_CLIENT} --user "${READER}" -q "SELECT * FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --user "${READER}" -q "DESCRIBE TABLE ${TABLE}" | cut -f1,2
${CLICKHOUSE_CLIENT} --user "${READER}" -q "INSERT INTO ${TABLE} SETTINGS s3_truncate_on_insert = 1 VALUES (4)" 2>&1 | grep -o -m1 'grant WRITE ON S3'
${CLICKHOUSE_CLIENT} --user "${READER}" --async_insert 1 --wait_for_async_insert 0 -q "INSERT INTO ${TABLE} SETTINGS s3_truncate_on_insert = 1 VALUES (4)" 2>&1 | grep -o -m1 'grant WRITE ON S3'
${CLICKHOUSE_CLIENT} --user "${READER}" -q "TRUNCATE TABLE ${TABLE}" 2>&1 | grep -o -m1 'grant WRITE ON S3'
${CLICKHOUSE_CLIENT} --user "${READER}" -q "SELECT * FROM ${TABLE}"

echo '--- READ and WRITE ON S3'
${CLICKHOUSE_CLIENT} --user "${READER_WRITER}" -q "INSERT INTO ${TABLE} SETTINGS s3_truncate_on_insert = 1 VALUES (5)"
${CLICKHOUSE_CLIENT} --user "${READER_WRITER}" -q "SELECT * FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --user "${READER_WRITER}" --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO ${TABLE} SETTINGS s3_truncate_on_insert = 1 VALUES (6)"
${CLICKHOUSE_CLIENT} --user "${READER_WRITER}" -q "SELECT * FROM ${TABLE}"

echo '--- WRITE ON S3 for f.csv only'
${CLICKHOUSE_CLIENT} --user "${FILTERED_WRITER}" -q "INSERT INTO ${TABLE} SETTINGS s3_create_new_file_on_insert = 1 VALUES (7)" 2>&1 | grep -o -m1 'grant WRITE ON S3'
${CLICKHOUSE_CLIENT} --user "${READER_WRITER}" -q "TRUNCATE TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('${PREFIX}*', 'test', 'testtest', 'CSV', 'c1 UInt64')"

${CLICKHOUSE_CLIENT} -q "
DROP USER ${READER}, ${WRITER}, ${READER_WRITER}, ${FILTERED_WRITER};
DROP DATABASE ${DB};
"
