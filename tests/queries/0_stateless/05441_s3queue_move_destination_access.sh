#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: the `S3Queue` and `AzureQueue` engines are not built into the fast test build.
# Tag zookeeper: `S3Queue` keeps its state in Keeper, and `ON CLUSTER` queries use the distributed DDL queue.

# The move destination of an `S3Queue` or `AzureQueue` table is the same data flow as
# `INSERT INTO FUNCTION s3('<destination>') SELECT * FROM <table>`: the table reads the processed files with its
# own credentials and writes them to the destination. So setting `after_processing_move_uri` or
# `after_processing_move_connection_string` needs `WRITE` on the `S3` (`AZURE`) source for the destination.
# `ALTER` needs `SELECT` on the table too. Settings that do not choose a destination need no extra grant.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user_alter="s3queue_move_alter_${db}"
user_create="s3queue_move_create_${db}"
keeper_path="/s3queue_move_destination_access_${db}"
src="http://localhost:11111/test/${db}_move_src/"
dst="http://localhost:11111/test/${db}_move_dst/"
other="http://localhost:11111/test/${db}_move_other/"
keys="after_processing_move_access_key_id = 'clickhouse', after_processing_move_secret_access_key = 'clickhouse'"
azure_connection_string="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${db}.t SYNC;
    DROP TABLE IF EXISTS ${db}.t_create SYNC;
    DROP USER IF EXISTS ${user_alter}, ${user_create};
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${db}.t (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}', after_processing = 'keep'
"
echo "table created"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${user_alter};
    GRANT ALTER SETTINGS ON ${db}.t TO ${user_alter};
    CREATE USER ${user_create};
    GRANT CREATE TABLE ON ${db}.* TO ${user_create};
    GRANT TABLE ENGINE ON S3Queue TO ${user_create};
    GRANT TABLE ENGINE ON AzureQueue TO ${user_create};
"

echo "-- ALTER with only ALTER SETTINGS"

$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING s3queue_after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

# Settings that do not choose a destination need only ALTER SETTINGS.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING polling_max_timeout_ms = 20000, after_processing_move_prefix = 'prefix_${db}'
" && echo "settings that are not a destination: allowed"

echo "-- ALTER with SELECT on the table, but no WRITE on S3"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${db}.t TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING s3queue_after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- ALTER ON CLUSTER is rejected by the initiator"

$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- ALTER with WRITE on S3 limited to another URL"

$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3('${other}.*') TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- ALTER with WRITE on S3"

$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3 TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
" && echo "destination with WRITE on S3: allowed"

# Resetting the destination disables the move, so it needs no grant for a destination.
$CLICKHOUSE_CLIENT -q "REVOKE WRITE ON S3 FROM ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = ''
" && echo "empty destination without WRITE on S3: allowed"

echo "-- ALTER with WRITE on S3 limited to the destination URL"

$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3('${dst}.*') TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
" && echo "destination inside the grant: allowed"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${other}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- CREATE without WRITE on S3 or AZURE"

$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = 'move',
        s3queue_after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create (x UInt8) ENGINE = AzureQueue('https://account.blob.core.windows.net', 'container', '*', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = 'move',
        after_processing_move_connection_string = '${azure_connection_string}', after_processing_move_container = 'dst'
    -- { serverError ACCESS_DENIED }
"

# The same CREATE without a destination, or with an empty one, needs no grant for it.
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = 'keep',
        after_processing_move_uri = ''
" && echo "table without a destination: created"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${db}.t SYNC;
    DROP TABLE IF EXISTS ${db}.t_create SYNC;
    DROP USER IF EXISTS ${user_alter}, ${user_create};
"
