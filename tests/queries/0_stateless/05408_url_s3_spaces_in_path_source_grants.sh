#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: creates a user

# A filtered source grant matches a URL with spaces the same way, whether the spaces are written
# literally or as `%20`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05408_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT READ ON S3('http://localhost:11111/test/05408%20allowed/.*') TO $user;
GRANT READ ON URL('http://localhost:11111/test/05408%20allowed/.*') TO $user;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05408 allowed/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05408 denied/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 2 SETTINGS s3_truncate_on_insert = 1;
EOF

echo 's3'
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM s3('http://localhost:11111/test/05408 allowed/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32')"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM s3('http://localhost:11111/test/05408%20allowed/a%20b.csv', 'test', 'testtest', 'CSV', 'x UInt32')"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM s3('http://localhost:11111/test/05408 denied/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32') -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM s3('http://localhost:11111/test/05408%20denied/a%20b.csv', 'test', 'testtest', 'CSV', 'x UInt32') -- { serverError ACCESS_DENIED }"

echo 'url'
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM url('http://localhost:11111/test/05408 allowed/a b.csv', 'CSV', 'x UInt32')"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM url('http://localhost:11111/test/05408%20allowed/a%20b.csv', 'CSV', 'x UInt32')"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM url('http://localhost:11111/test/05408 denied/a b.csv', 'CSV', 'x UInt32') -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT x FROM url('http://localhost:11111/test/05408%20denied/a%20b.csv', 'CSV', 'x UInt32') -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
