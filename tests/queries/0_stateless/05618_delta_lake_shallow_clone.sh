#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-replicated-database
# Tag no-fasttest: delta-kernel pulls in extra dependencies, and MinIO is not in fast test.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.
# Tag no-replicated-database: without enable_read_write_grants, replicated access storage drops grant filters

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/130550
# A Delta Lake shallow clone references the data files and deletion vectors of its source table by absolute URI,
# which Spark writes as `file:/<path>` or `s3a://<bucket>/<key>`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="${CUR_DIR}/data_delta_lake/kernel_data/table-with-dv-small"
PARQUET="part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
DV="deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
USER_DIR="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
rm -rf "$DIR" "${USER_DIR}_src" "${USER_DIR}_clone" "${USER_DIR}_escape"

# The log of a clone of `table-with-dv-small`: its protocol and metadata, its data file at the URI $2,
# and its deletion vector at the URI $3 if given.
write_clone_log()
{
    local dv=""
    if [ -n "$3" ]; then
        dv=",\"deletionVector\":{\"storageType\":\"p\",\"pathOrInlineDv\":\"$3\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":2}"
    fi
    mkdir -p "$1/_delta_log"
    {
        grep -E '^\{"(protocol|metaData)"' "$SRC/_delta_log/00000000000000000000.json"
        echo "{\"add\":{\"path\":\"$2\",\"partitionValues\":{},\"size\":635,\"modificationTime\":1677811178336,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":10}\"$dv}}"
    } > "$1/_delta_log/00000000000000000000.json"
}

echo '-- local, with a deletion vector'
write_clone_log "$DIR/clone" "file:$SRC/$PARQUET" "file:$SRC/$DV"
$CLICKHOUSE_LOCAL -q "SELECT arraySort(groupArray(value)), count() FROM deltaLakeLocal('$DIR/clone')"

echo '-- local, file:// URI'
write_clone_log "$DIR/clone_uri" "file://$SRC/$PARQUET"
$CLICKHOUSE_LOCAL -q "SELECT sum(value), count() FROM deltaLakeLocal('$DIR/clone_uri')"

echo '-- server, source inside user_files'
cp -r "$SRC" "${USER_DIR}_src"
write_clone_log "${USER_DIR}_clone" "file:${USER_DIR}_src/$PARQUET"
$CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM deltaLakeLocal('${USER_DIR}_clone')"

echo '-- server, source outside user_files'
write_clone_log "${USER_DIR}_escape" "file:$SRC/$PARQUET"
$CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM deltaLakeLocal('${USER_DIR}_escape')" 2>&1 \
    | grep -o -m1 -E 'is outside the table path directory|PATH_ACCESS_DENIED'

echo '-- s3'
S3_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
upload()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename = '$S3_DIR/$1', format = RawBLOB)
        SETTINGS s3_truncate_on_insert = 1 FORMAT RawBLOB" < "$2"
}
write_clone_log "$DIR/s3_clone" "s3a://test/$S3_DIR/src/$PARQUET" "s3a://test/$S3_DIR/src/$DV"
upload "src/$PARQUET" "$SRC/$PARQUET"
upload "src/$DV" "$SRC/$DV"
upload "clone/_delta_log/00000000000000000000.json" "$DIR/s3_clone/_delta_log/00000000000000000000.json"
$CLICKHOUSE_CLIENT -q "SELECT arraySort(groupArray(value)), count() FROM deltaLake(s3_conn, filename = '$S3_DIR/clone/')"

echo '-- s3, another bucket'
write_clone_log "$DIR/s3_foreign" "s3a://other-bucket/$S3_DIR/src/$PARQUET"
upload "foreign/_delta_log/00000000000000000000.json" "$DIR/s3_foreign/_delta_log/00000000000000000000.json"
$CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM deltaLake(s3_conn, filename = '$S3_DIR/foreign/')" 2>&1 \
    | grep -o -m1 -E 'is not in the bucket|NOT_IMPLEMENTED'

echo '-- s3, gs:// path on S3'
write_clone_log "$DIR/s3_gs" "gs://test/$S3_DIR/src/$PARQUET"
upload "gs/_delta_log/00000000000000000000.json" "$DIR/s3_gs/_delta_log/00000000000000000000.json"
$CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM deltaLake(s3_conn, filename = '$S3_DIR/gs/')" 2>&1 \
    | grep -o -m1 -E 'is not in the bucket|NOT_IMPLEMENTED'

echo '-- a user granted only the clone'
user="u_05618_${CLICKHOUSE_DATABASE}"
s3_clone="http://localhost:11111/test/$S3_DIR/clone/"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT --multiquery -q "
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT READ ON FILE('${USER_DIR}_clone.*') TO $user;
GRANT READ ON S3('$s3_clone.*') TO $user;"
for f in "deltaLakeLocal('${USER_DIR}_clone')" "deltaLake('$s3_clone', 'test', 'testtest')"; do
    $CLICKHOUSE_CLIENT --user "$user" -q "DESCRIBE $f" | cut -f1
    $CLICKHOUSE_CLIENT --user "$user" -q "SELECT sum(value) FROM $f" 2>&1 \
        | grep -o -m1 -E 'necessary to have the grant READ ON (FILE|S3)'
done
$CLICKHOUSE_CLIENT -q "DROP USER $user"

rm -rf "$DIR" "${USER_DIR}_src" "${USER_DIR}_clone" "${USER_DIR}_escape"
