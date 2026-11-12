#!/usr/bin/env bash
# Tags: no-fasttest, long
# - no-fasttest: requires MinIO and the Avro library (Iceberg)
# - long: writing the more than 1000 objects of a full list page runs past the 180s flaky-check cap

# With `iceberg_list_only_v_prefixed_metadata_files`, a read of an Iceberg table by path, with no
# pointer setting, finds the latest metadata file by listing the `metadata/v` key prefix when the
# table names its metadata files `v<N>...`, so the other objects of the directory (manifests,
# manifest lists, orphans) cost no list requests, and a `<N>-<uuid>` file is not a candidate next to
# the `v` files. Without `v` files the whole directory is listed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --iceberg_list_only_v_prefixed_metadata_files=1"
PREFIX="05441_iceberg_v_prefix/${CLICKHOUSE_TEST_UNIQUE_NAME}"
UUID_A='6f6f6407-c6a5-465f-a808-ea8900e35a38'
UUID_B='a90eed4c-f74b-4e5b-b630-096fb9d09021'

copy_object()
{
    $CLIENT --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '$2', format = 'RawBLOB')
        SELECT * FROM s3(s3_conn, filename = '$1', format = 'RawBLOB')"
}

# More objects than one list page (1000 keys) that are not metadata files add no list requests.
P1="${PREFIX}/listing"
$CLIENT --query "
    CREATE TABLE t_listing (c Int32) ENGINE = IcebergS3(s3_conn, filename = '${P1}');
    INSERT INTO t_listing VALUES (1);
    INSERT INTO t_listing VALUES (2);
    INSERT INTO t_listing VALUES (3);"

select_sum()
{
    $CLIENT --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" \
        --query "SELECT '$1', sum(c) FROM icebergS3(s3_conn, filename = '${P1}')"
}

select_sum before
$CLIENT --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${P1}/metadata/orphan_{_partition_id}.avro', format = 'CSV', structure = 'n UInt32')
    PARTITION BY n SELECT number FROM numbers(1100)
    SETTINGS max_partitions_per_insert_block = 0, output_format_parallel_formatting = 0;
    SELECT 'orphans', count() FROM s3(s3_conn, filename = '${P1}/metadata/orphan_*', format = 'One');"
select_sum after
$CLIENT --query "INSERT INTO t_listing VALUES (4)"
select_sum after_insert

$CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLIENT --query "
    SELECT
        replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''),
        ProfileEvents['S3ListObjects'] > 0,
        ProfileEvents['S3ListObjects'] = (
            SELECT ProfileEvents['S3ListObjects'] FROM system.query_log
            WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
                AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_before')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND startsWith(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')
    ORDER BY event_time_microseconds"

# Next to the `v` files, a `<N>-<uuid>` file is not a candidate, even with a higher number.
P3="${PREFIX}/mixed"
$CLIENT --query "
    CREATE TABLE t_mixed (c Int32) ENGINE = IcebergS3(s3_conn, filename = '${P3}');
    INSERT INTO t_mixed VALUES (1);
    INSERT INTO t_mixed VALUES (2);"
copy_object "${P3}/metadata/v2.metadata.json" "${P3}/metadata/00005-${UUID_A}.metadata.json"
$CLIENT --query "
    SELECT 'mixed', count() FROM icebergS3(s3_conn, filename = '${P3}');"

# The copies of `${P3}`'s metadata files keep its location, which a read re-roots under the copy's
# directory, so its manifest lists and manifests are copied along.
copy_manifests()
{
    for f in $($CLIENT --query "SELECT _file FROM s3(s3_conn, filename = '${P3}/metadata/*.avro', format = 'One') ORDER BY _file"); do
        copy_object "${P3}/metadata/${f}" "$1/metadata/${f}"
    done
}

# A directory with only `<N>-<uuid>` files is listed whole. Its data files are not copied, so the
# count is taken from the snapshot summary.
P4="${PREFIX}/uuid_named"
copy_object "${P3}/metadata/v2.metadata.json" "${P4}/metadata/00001-${UUID_A}.metadata.json"
copy_object "${P3}/metadata/v3.metadata.json" "${P4}/metadata/00002-${UUID_B}.metadata.json"
copy_manifests "${P4}"
$CLIENT --use_iceberg_metadata_files_cache=0 --query "
    SELECT 'uuid-named', count() FROM icebergS3(s3_conn, filename = '${P4}')
    SETTINGS optimize_trivial_count_query = 1"

# Without `iceberg_list_only_v_prefixed_metadata_files` (the default) or ranking by `last-updated-ms`,
# every file is a candidate: the `<N>-<uuid>` file committed last is selected.
P6="${PREFIX}/by_last_updated_ms"
copy_object "${P3}/metadata/v2.metadata.json" "${P6}/metadata/v1.metadata.json"
copy_object "${P3}/metadata/v3.metadata.json" "${P6}/metadata/00002-${UUID_B}.metadata.json"
copy_manifests "${P6}"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT 'default', count() FROM icebergS3(s3_conn, filename = '${P6}')
    SETTINGS optimize_trivial_count_query = 1"
$CLIENT --use_iceberg_metadata_files_cache=0 --query "
    SELECT 'v file only', count() FROM icebergS3(s3_conn, filename = '${P6}')
    SETTINGS optimize_trivial_count_query = 1;
    SELECT 'all files', count() FROM icebergS3(s3_conn, filename = '${P6}')
    SETTINGS optimize_trivial_count_query = 1, iceberg_list_only_v_prefixed_metadata_files = 0;
    SELECT 'by last-updated-ms', count()
    FROM icebergS3(s3_conn, filename = '${P6}', SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1)
    SETTINGS optimize_trivial_count_query = 1;"

# DROP PARTITION resolves like INSERT: a directory holding both namings is refused and left as it is.
P5="${PREFIX}/drop_partition"
$CLIENT --query "
    CREATE TABLE t_drop (a Int32, s String) ENGINE = IcebergS3(s3_conn, filename = '${P5}') PARTITION BY (a);
    INSERT INTO t_drop VALUES (1, 'x'), (2, 'y');
    INSERT INTO t_drop VALUES (3, 'z');"
copy_object "${P5}/metadata/v2.metadata.json" "${P5}/metadata/00005-${UUID_A}.metadata.json"
$CLIENT --query "SELECT 'before drop', a, s FROM t_drop ORDER BY a"
$CLIENT --query "ALTER TABLE t_drop DROP PARTITION 1" 2>&1 | grep -q "nothing declares which of the two schemes" && echo 'drop refused'
$CLIENT --query "
    SELECT 'after drop', a, s FROM t_drop ORDER BY a;
    SELECT 'v files', _file FROM s3(s3_conn, filename = '${P5}/metadata/v*.metadata.json', format = 'One') ORDER BY _file;"

$CLIENT --query "DROP TABLE t_listing; DROP TABLE t_mixed; DROP TABLE t_drop;"
