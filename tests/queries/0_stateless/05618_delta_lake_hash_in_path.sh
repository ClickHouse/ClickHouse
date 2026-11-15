#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test, depends on S3 and Azure
# Tag no-msan: delta-kernel-rs is not built with MSan

# A Delta Lake table whose path contains `#` must be read from and committed to its own `_delta_log`, not to the log
# of the table at the part of the path before the `#`. Every table below is a byte copy of one of two template tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"
S3="http://localhost:11111/test/${PREFIX}"
AZURE="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"

rm -rf "$DIR"
mkdir -p "$DIR"

# `one` is the neighbour, `two` is the table a query names.
$CLICKHOUSE_CLIENT -m -q "
SET allow_delta_lake_writes = 1, allow_delta_lake_create_table = 1;
CREATE TABLE t_one (id Int32, s String) ENGINE = DeltaLakeLocal('$DIR/one', Parquet);
INSERT INTO t_one VALUES (1, 'sibling');
CREATE TABLE t_two (id Int32, s String) ENGINE = DeltaLakeLocal('$DIR/two', Parquet);
INSERT INTO t_two VALUES (2, 'intended'), (3, 'intended');
DROP TABLE t_one;
DROP TABLE t_two;
"

# Print the statements copying template $1 to the S3 key $2 (URL-encoded) or to the Azure blob path $2.
function copy_s3()
{
    (cd "$DIR/$1" && find . -type f -printf '%P\n') | while read -r file; do
        echo "INSERT INTO FUNCTION s3('$S3/$2/$file', 'test', 'testtest', 'RawBLOB') SELECT * FROM file('$DIR/$1/$file', 'RawBLOB');"
    done
}

function copy_azure()
{
    (cd "$DIR/$1" && find . -type f -printf '%P\n') | while read -r file; do
        echo "INSERT INTO FUNCTION azureBlobStorage('$AZURE', 'cont', '$PREFIX/$2/$file', 'RawBLOB') SELECT * FROM file('$DIR/$1/$file', 'RawBLOB');"
    done
}

$CLICKHOUSE_CLIENT -m -q "
$(copy_s3 one x)
$(copy_s3 two x/%23t)
$(copy_azure two 'x#t')
"

cp -r "$DIR/one" "$DIR/x"
cp -r "$DIR/two" "$DIR/x#t"

function check()
{
    $CLICKHOUSE_CLIENT -m -q "SELECT count() FROM $1; SELECT arraySort(groupArray(s)) FROM $1;"
}

echo '-- s3'
check "deltaLake('$S3/x/%23t/', 'test', 'testtest')"

echo '-- azure'
check "deltaLakeAzure('$AZURE', 'cont', '$PREFIX/x#t/')"

echo '-- local'
check "deltaLakeLocal('$DIR/x#t')"

echo '-- insert'
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t_insert ENGINE = DeltaLake('$S3/x/%23t/', 'test', 'testtest');
INSERT INTO t_insert SETTINGS allow_delta_lake_writes = 1 VALUES (4, 'inserted');
DROP TABLE t_insert;
"
check "deltaLake('$S3/x/%23t/', 'test', 'testtest')"
check "deltaLake('$S3/x/', 'test', 'testtest')"

rm -rf "$DIR"
