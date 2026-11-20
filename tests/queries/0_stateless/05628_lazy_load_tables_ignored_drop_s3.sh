#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: S3 is not available in the fast test.
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# With `ignore_drop_queries_probability` a `DROP` is either skipped or replaced by a `TRUNCATE`, and a table whose
# real `DROP` keeps its data, such as an `S3` table, must not be truncated. The stand-in of a deferred table does not
# know that it is an object storage, so the decision has to be made on the real storage: otherwise the objects in
# S3 are deleted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.t (x UInt64) ENGINE = S3(s3_conn, filename = '${CLICKHOUSE_DATABASE}_05628.csv', format = CSV);
    INSERT INTO \`$DB\`.t SETTINGS s3_truncate_on_insert = 1 VALUES (7);"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "SELECT is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"

$CLICKHOUSE_CLIENT -q "DROP TABLE \`$DB\`.t SETTINGS ignore_drop_queries_probability = 1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM \`$DB\`.t"

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
