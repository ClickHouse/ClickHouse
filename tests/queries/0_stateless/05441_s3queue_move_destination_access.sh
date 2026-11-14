#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper, no-replicated-database
# Tag no-fasttest: the `S3Queue` and `AzureQueue` engines are not built into the fast test build.
# Tag zookeeper: `S3Queue` keeps its state in Keeper, and `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-replicated-database: with `access_control_improvements.enable_read_write_grants` disabled (the default),
# a replicated access storage round-trips the user through serialized SQL, which rewrites `WRITE ON S3('<filter>')`
# into the old-style whole-source `S3` grant without the filter.

# The move destination of an `S3Queue` or `AzureQueue` table is the same data flow as
# `INSERT INTO FUNCTION s3('<destination>') SELECT * FROM <table>`: the table reads the processed files with its
# own credentials and writes them to the destination. So setting `after_processing_move_uri`,
# `after_processing_move_connection_string` or `after_processing_move_container` needs `WRITE` on the `S3` (`AZURE`)
# source without a URL filter, because the move writes keys built from the prefix and the source keys, not from the URI.
# `ALTER` needs `SELECT` on the table too. Settings that do not choose a destination need no extra grant.
# `after_processing` = `delete`, `move` or `tag` modifies the source files, so it also needs `WRITE` on the source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user_alter="s3queue_move_alter_${db}"
user_create="s3queue_move_create_${db}"
keeper_path="/s3queue_move_destination_access_${db}"
src="http://localhost:11111/test/${db}_move_src/"
dst="http://localhost:11111/test/${db}_move_dst/"
backup="Disk('backups', '05441_${CLICKHOUSE_TEST_UNIQUE_NAME}')"
backup_src="Disk('backups', '05441_${CLICKHOUSE_TEST_UNIQUE_NAME}_src')"
keys="after_processing_move_access_key_id = 'clickhouse', after_processing_move_secret_access_key = 'clickhouse'"
attach_uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
azure_connection_string="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${db}.t_alias SYNC;
    DROP TABLE IF EXISTS ${db}.t SYNC;
    DROP TABLE IF EXISTS ${db}.t_create SYNC;
    DROP TABLE IF EXISTS ${db}.t_delete SYNC;
    DROP TABLE IF EXISTS ${db}.t_src SYNC;
    DROP TABLE IF EXISTS ${db}.t_src_other SYNC;
    DROP TABLE IF EXISTS ${db}.t_as SYNC;
    DROP TABLE IF EXISTS ${db}.t_as_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.t_as_cluster_new SYNC;
    DROP TABLE IF EXISTS ${db}.t_create_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.t_attach SYNC;
    DROP TABLE IF EXISTS ${db}.t_backup SYNC;
    DROP TABLE IF EXISTS ${db}.t_backup_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.mv SYNC;
    DROP ROW POLICY IF EXISTS p_${db} ON ${db}.t;
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

# Changing only the container redirects the move to another container of the same account.
# The check reads only the query, so it fails before the storage is involved, although `t` is an `S3Queue`.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_container = 'dst'
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

# Changing only the container is a destination change that needs `WRITE` on `AZURE`.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_container = 'dst'
    -- { serverError ACCESS_DENIED }
"

# `delete`, `move` and `tag` modify the source files, so they need `WRITE` on `S3` too.
for action in delete move tag; do
    $CLICKHOUSE_CLIENT --user "${user_alter}" -q "
        ALTER TABLE ${db}.t MODIFY SETTING after_processing = '${action}'
        -- { serverError ACCESS_DENIED }
    "
done

echo "-- ALTER ON CLUSTER is rejected by the initiator"

$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost MODIFY SETTING after_processing = 'delete'
    -- { serverError ACCESS_DENIED }
"

echo "-- ALTER ON CLUSTER of a table that does not exist on the initiator"

# The grants come from the settings in the query, not from the table, so the initiator checks them without the table.
# With the grant, the query reaches the host and fails there.
$CLICKHOUSE_CLIENT -q "GRANT ALTER SETTINGS, SELECT ON ${db}.t_missing TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t_missing ON CLUSTER test_shard_localhost MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3 TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" --distributed_ddl_output_mode none -q "
    ALTER TABLE ${db}.t_missing ON CLUSTER test_shard_localhost MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError UNKNOWN_TABLE }
"
$CLICKHOUSE_CLIENT -q "REVOKE WRITE ON S3 FROM ${user_alter}"

echo "-- ALTER with WRITE on S3 limited by a URL filter"

# The move writes keys built from the prefix and the source keys, so a URL filter cannot authorize them.
$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3('${dst}.*') TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT -q "REVOKE WRITE ON S3('${dst}.*') FROM ${user_alter}"

echo "-- ALTER with WRITE on S3"

$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3 TO ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
" && echo "destination with WRITE on S3: allowed"

# `AzureQueue` also has `after_processing`, but an `S3Queue` table needs no grant on `AZURE`.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing = 'delete'
" && echo "after_processing = 'delete' with WRITE on S3: allowed"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${db}.t MODIFY SETTING after_processing = 'keep'"
# With `ON CLUSTER` the table on another host may be an `AzureQueue`, so both grants are needed.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost MODIFY SETTING after_processing = 'delete'
    -- { serverError ACCESS_DENIED }
"

# Resetting the destination disables the move, so it needs no grant for a destination.
$CLICKHOUSE_CLIENT -q "REVOKE WRITE ON S3 FROM ${user_alter}"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = ''
" && echo "empty destination without WRITE on S3: allowed"

echo "-- ALTER with WRITE on S3, restricted by a row policy"

# The move copies whole files, so a user whose row policy hides rows of the table cannot set a destination.
$CLICKHOUSE_CLIENT -q "
    GRANT WRITE ON S3 TO ${user_alter};
    CREATE ROW POLICY p_${db} ON ${db}.t USING x = 1 TO ${user_alter};
"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t ON CLUSTER test_shard_localhost MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"
# Turning the move off needs no check.
$CLICKHOUSE_CLIENT --user "${user_alter}" -q "
    ALTER TABLE ${db}.t MODIFY SETTING after_processing_move_uri = ''
" && echo "empty destination with a row policy: allowed"
$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY p_${db} ON ${db}.t;
    REVOKE WRITE ON S3 FROM ${user_alter};
"

echo "-- ALTER through an Alias"

# The export checks see only the name in the query, which is the alias, so the change is refused for every user.
$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "CREATE TABLE ${db}.t_alias ENGINE = Alias('${db}', 't')"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE ${db}.t_alias MODIFY SETTING after_processing_move_uri = '${dst}', ${keys}
    -- { serverError NOT_IMPLEMENTED }
"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE ${db}.t_alias MODIFY SETTING polling_max_timeout_ms = 20000
" && echo "settings that are not a destination through an alias: allowed"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.t_alias SYNC"

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
# `CREATE` with `delete`, `move` or `tag` needs `WRITE` on the source too.
for action in delete move tag; do
    $CLICKHOUSE_CLIENT --user "${user_create}" -q "
        CREATE TABLE ${db}.t_create (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
        SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = '${action}',
            after_processing_move_prefix = 'p', after_processing_tag_key = 'k', after_processing_tag_value = 'v'
        -- { serverError ACCESS_DENIED }
    "
done

# The same CREATE without a destination, or with an empty one, needs no grant for it.
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create', after_processing = 'keep',
        after_processing_move_uri = ''
" && echo "table without a destination: created"

echo "-- CREATE AS with a destination, without WRITE on S3"

# The engine and the settings of `AS` are inherited after the query is parsed, so the check has to see the final definition.
# `SELECT` is required because the definition of the source has secret parts.
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS, SELECT ON ${db}.t TO ${user_create}"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_as AS ${db}.t
    SETTINGS keeper_path = '${keeper_path}_as', after_processing = 'move', after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- CREATE MATERIALIZED VIEW with a queue inner engine, without WRITE on S3"

# The engine is an inner engine of the view, and the inner table is created as an internal query, which checks nothing.
$CLICKHOUSE_CLIENT -q "GRANT CREATE VIEW ON ${db}.* TO ${user_create}"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE MATERIALIZED VIEW ${db}.mv ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_mv', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
    AS SELECT 1::UInt8 AS x
    -- { serverError ACCESS_DENIED }
"

echo "-- CREATE AS ON CLUSTER with the legacy entry format, without WRITE on S3"

# This format ships the query as written and the workers resolve `AS` with no user.
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${user_create}"
$CLICKHOUSE_CLIENT --user "${user_create}" --distributed_ddl_entry_format_version 2 -q "
    CREATE TABLE ${db}.t_as_cluster ON CLUSTER test_shard_localhost AS ${db}.t
    SETTINGS keeper_path = '${keeper_path}_as_cluster', after_processing = 'move', after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- CREATE AS ON CLUSTER with the default entry format, without WRITE on S3"

# This format builds the definition on the initiator.
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_as_cluster_new ON CLUSTER test_shard_localhost AS ${db}.t
    SETTINGS keeper_path = '${keeper_path}_as_cluster_new', after_processing = 'move', after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- CREATE ON CLUSTER with a queue engine and a destination, without WRITE on S3"

$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_create_cluster ON CLUSTER test_shard_localhost (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create_cluster', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- ATTACH with a full definition and a destination, without WRITE on S3"

# A full definition in `ATTACH` is a new definition, like `CREATE`. An `Atomic` database needs a `UUID` for it.
# The server warns that a full definition in `ATTACH` is not recommended, so only errors are sent to the client.
$CLICKHOUSE_CLIENT --user "${user_create}" --send_logs_level=error -q "
    ATTACH TABLE ${db}.t_attach UUID '${attach_uuid}' (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_attach', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
    -- { serverError ACCESS_DENIED }
"

echo "-- RESTORE of a queue with a destination, without WRITE on S3"

# The restored definition comes from the backup, and the table is created by an internal query,
# so the restore has to authorize the destination before it creates anything.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${db}.t_backup (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_backup', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE ${db}.t_backup TO ${backup} FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.t_backup SYNC"
$CLICKHOUSE_CLIENT -q "GRANT READ ON DISK TO ${user_create}"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    RESTORE TABLE ${db}.t_backup FROM ${backup} SETTINGS structure_only = 1 FORMAT Null
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${db}' AND name = 't_backup'"

# The hosts create the tables with no user, so the initiator must authorize the destination.
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    RESTORE TABLE ${db}.t_backup AS ${db}.t_backup_cluster ON CLUSTER test_shard_localhost FROM ${backup}
    SETTINGS structure_only = 1 FORMAT Null
    -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${db}' AND name = 't_backup_cluster'"

# `RESTORE` is checked like `CREATE`: a queue with a different source cannot use an existing `keeper_path`.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${db}.t_src (x UInt8) ENGINE = S3Queue('${dst}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_src';
    BACKUP TABLE ${db}.t_src TO ${backup_src} FORMAT Null;
    DROP TABLE ${db}.t_src SYNC;
    CREATE TABLE ${db}.t_src_other (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_src';
"
$CLICKHOUSE_CLIENT -q "
    RESTORE TABLE ${db}.t_src FROM ${backup_src} SETTINGS structure_only = 1 FORMAT Null
    -- { serverError METADATA_MISMATCH }
"

echo "-- CREATE AS and CREATE MATERIALIZED VIEW with WRITE on S3"

# The same queries succeed with the grant, so `WRITE` on `S3` was the only missing one.
$CLICKHOUSE_CLIENT -q "GRANT WRITE ON S3 TO ${user_create}"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_as AS ${db}.t
    SETTINGS keeper_path = '${keeper_path}_as', after_processing = 'move', after_processing_move_uri = '${dst}', ${keys}
" && echo "CREATE AS with WRITE on S3: created"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE TABLE ${db}.t_delete (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_delete', after_processing = 'delete'
" && echo "after_processing = 'delete' with WRITE on S3: created"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    CREATE MATERIALIZED VIEW ${db}.mv ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_mv', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
    AS SELECT 1::UInt8 AS x
" && echo "materialized view with WRITE on S3: created"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    RESTORE TABLE ${db}.t_backup FROM ${backup} SETTINGS structure_only = 1 FORMAT Null
" && echo "RESTORE with WRITE on S3: restored"
$CLICKHOUSE_CLIENT --user "${user_create}" -q "
    RESTORE TABLE ${db}.t_backup AS ${db}.t_backup_cluster ON CLUSTER test_shard_localhost FROM ${backup}
    SETTINGS structure_only = 1 FORMAT Null
" && echo "RESTORE ON CLUSTER with WRITE on S3: restored"
$CLICKHOUSE_CLIENT --user "${user_create}" --distributed_ddl_output_mode none -q "
    CREATE TABLE ${db}.t_create_cluster ON CLUSTER test_shard_localhost (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_create_cluster', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
" && echo "CREATE ON CLUSTER with WRITE on S3: created"
$CLICKHOUSE_CLIENT --user "${user_create}" --distributed_ddl_output_mode none -q "
    CREATE TABLE ${db}.t_as_cluster_new ON CLUSTER test_shard_localhost AS ${db}.t
    SETTINGS keeper_path = '${keeper_path}_as_cluster_new', after_processing = 'move', after_processing_move_uri = '${dst}', ${keys}
" && echo "CREATE AS ON CLUSTER with WRITE on S3: created"
$CLICKHOUSE_CLIENT --user "${user_create}" --send_logs_level=error -q "
    ATTACH TABLE ${db}.t_attach UUID '${attach_uuid}' (x UInt8) ENGINE = S3Queue('${src}', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${keeper_path}_attach', after_processing = 'move',
        after_processing_move_uri = '${dst}', ${keys}
" && echo "ATTACH with WRITE on S3: attached"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${db}.t_alias SYNC;
    DROP TABLE IF EXISTS ${db}.t SYNC;
    DROP TABLE IF EXISTS ${db}.t_create SYNC;
    DROP TABLE IF EXISTS ${db}.t_delete SYNC;
    DROP TABLE IF EXISTS ${db}.t_src SYNC;
    DROP TABLE IF EXISTS ${db}.t_src_other SYNC;
    DROP TABLE IF EXISTS ${db}.t_as SYNC;
    DROP TABLE IF EXISTS ${db}.t_as_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.t_as_cluster_new SYNC;
    DROP TABLE IF EXISTS ${db}.t_create_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.t_attach SYNC;
    DROP TABLE IF EXISTS ${db}.t_backup SYNC;
    DROP TABLE IF EXISTS ${db}.t_backup_cluster SYNC;
    DROP TABLE IF EXISTS ${db}.mv SYNC;
    DROP ROW POLICY IF EXISTS p_${db} ON ${db}.t;
    DROP USER IF EXISTS ${user_alter}, ${user_create};
"
