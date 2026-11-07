#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Invalid metadata that has to be refused with an error.
# The tables and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

# Prints the error code of a query that has to fail.
function error_code()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -o -m1 'Code: [0-9]*'
}

# For metadata that has to be refused: prints `rejected` for an exception, but not for a raw Poco exception,
# a logical error, a missing file or a rejected metadata file name, which are not the reason to refuse it.
function rejected()
{
    local error
    error=$($CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -m1 'Code: ')
    if [ -z "$error" ]; then
        echo 'not rejected'
    elif grep -q -E 'Bad metadata file name|\((POCO_EXCEPTION|LOGICAL_ERROR|FILE_DOESNT_EXIST)\)' <<< "$error"; then
        echo 'unexpected error'
    else
        echo 'rejected'
    fi
}

# Format version 4 is not supported.
echo "format_version_4 $(error_code "SELECT * FROM icebergLocal('data/persistent/format_version_4')")"

# A v2 table without `schema-id` in its schema.
echo "missing_schema_id $(error_code "SELECT * FROM icebergLocal('data/persistent/missing_schema_id')")"

# Bug metadata-validation: format version 0 is accepted, a missing `location` gives POCO_EXCEPTION,
# missing `last-updated-ms` and `spec-id` (of a partition spec) are accepted.
for t in invalid_format_version missing_location missing_last_updated_ms missing_spec_id
do
    echo "$t $(rejected "SELECT * FROM icebergLocal('data/persistent/$t')")"
done

# A v1 table whose manifest list references a v2 manifest (of the table `partition_float`).
# Bug path-resolver: the path of the manifest is mangled to `ata/persistent/...`.
echo "manifest_newer_than_table $(rejected "SELECT * FROM icebergLocal('data/persistent/manifest_newer_than_table')")"

# Malformed decimal types: decimal(10), decimal(a,b), precision above 38, precision 0, scale above the precision.
# Bug metadata-file-names: `decimal_missing_scale.metadata.json` and so on.
for f in decimal_missing_scale decimal_not_numeric decimal_too_wide decimal_zero_precision decimal_scale_exceeds_precision
do
    echo "invalid_decimal_type $f $(rejected "SELECT * FROM icebergLocal('data/persistent/invalid_decimal_type', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json')")"
done

# The manifest list (with the manifests of the table `partition_bool`) references the partition spec 999 or -1,
# which is not in the metadata.
# Bugs metadata-file-names (`spec_id_999.metadata.json`), path-resolver (the manifests are outside of the directory
# of the table) and unknown-spec-id.
for f in spec_id_999 spec_id_negative
do
    echo "manifest_list_invalid_spec_id $f $(rejected "SELECT * FROM icebergLocal('data/persistent/manifest_list_invalid_spec_id', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json')")"
done
