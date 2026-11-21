#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: its replicated access storage drops the filters of the source grants

# The numbered keys of an insert split by size, such as `data.1.tsv` for `data.tsv`, are checked against the
# `WRITE ON S3` grant of the writer before they are written, and before a truncating insert deletes them:
# a grant that allows only the base key does not allow the numbered keys next to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05828_${CLICKHOUSE_DATABASE}"
PREFIX="05828_split_on_write_grant/${CLICKHOUSE_DATABASE}"
URL="http://localhost:11111/test/${PREFIX}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --multiquery -q "
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT READ ON S3 TO $user;
GRANT WRITE ON S3('${URL}/data[.]tsv') TO $user;
GRANT WRITE ON S3('${URL}/part/p[^/]+/data[.]tsv') TO $user;
"

# The grant of the partitioned insert matches any partition directory, so it allows both the path template
# `p{_partition_id}/data.tsv` (checked when the table function is executed) and the base key of every partition,
# but still not their numbered keys such as `p0/data.1.tsv`.

# Every block is exactly 100 numbers, and a new object is started as soon as 100 bytes are written,
# so an insert of more than one block rolls over into a numbered object.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 100"

function as_user()
{
    ${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'OK'
}

function objects()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT replaceOne(_path, 'test/${PREFIX}/', '') AS key, count(), min(x), max(x) FROM s3('${URL}/$1', 'test', 'testtest', 'TSV', 'x UInt64') GROUP BY key ORDER BY key"
}

echo '--- A rollover into a numbered key outside the grant is refused'
as_user "INSERT INTO FUNCTION s3('${URL}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
objects 'data*.tsv'

echo '--- A truncating split insert cannot delete a numbered object outside the grant'
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3('${URL}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
    INSERT INTO FUNCTION s3('${URL}/data.1.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT 2 SETTINGS s3_truncate_on_insert = 1;
"
as_user "INSERT INTO FUNCTION s3('${URL}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT 100 SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1"
objects 'data*.tsv'

echo '--- A rollover of a partition into a numbered key outside the grant is refused, the base objects are written'
as_user "INSERT INTO FUNCTION s3('${URL}/part/p{_partition_id}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') PARTITION BY x % 2 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
objects 'part/*/data*.tsv'

echo '--- With a grant covering the numbered keys, the same inserts succeed'
${CLICKHOUSE_CLIENT} --multiquery -q "
GRANT WRITE ON S3('${URL}/data.*') TO $user;
GRANT WRITE ON S3('${URL}/part/.*') TO $user;
"
as_user "INSERT INTO FUNCTION s3('${URL}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT 100 SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1"
objects 'data*.tsv'
as_user "INSERT INTO FUNCTION s3('${URL}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') SELECT number FROM numbers(300) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1"
objects 'data*.tsv'
as_user "INSERT INTO FUNCTION s3('${URL}/part/p{_partition_id}/data.tsv', 'test', 'testtest', 'TSV', 'x UInt64') PARTITION BY x % 2 SELECT number FROM numbers(400) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1"
objects 'part/*/data*.tsv'
