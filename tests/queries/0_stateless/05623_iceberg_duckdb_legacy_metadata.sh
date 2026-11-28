#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Deprecated and optional fields of the metadata: v1 `schema`, `partition-spec` and `snapshot.manifests`,
# partition fields without field ids, snapshots without `schema-id`, v1 manifests without optional keys,
# v1 manifest entries with the status EXISTING, tables upgraded to v3, and row lineage.
# The tables and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

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

LEGACY='data/persistent/iceberg_v1_deprecated/default/legacy_v1'

# A v1 table that has only the deprecated `schema` and `partition-spec` (a bare list of fields), and a snapshot
# with embedded `manifests` instead of a `manifest-list`. `v1.metadata.json` is the same table before the first insert.
# Bugs optional-schema-id (`v1.metadata.json`) and metadata-validation (embedded manifests: POCO_EXCEPTION).
for f in v1 v2
do
    $CLICKHOUSE_LOCAL -q "
    SELECT 'legacy_v1 $f';
    SELECT * FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json') ORDER BY id;
    SELECT id, amount FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json') WHERE category = 'beta';
    "
done

# The same snapshot with variations of the metadata. Bug metadata-file-names for all of them.
# - The fields of the partition spec have no field ids: they are assigned 1000, 1001, ... by position,
#   in the deprecated `partition-spec` and in `partition-specs`.
# - The snapshot has no `schema-id` (bug optional-schema-id).
# - The v1 manifest has no `format-version`, `partition-spec-id` and `schema-id` keys; its own `partition-spec`
#   matches the partition spec of the table.
for f in no-partition-field-ids no-partition-field-ids-in-specs no-snapshot-schema-id no-optional-manifest-keys
do
    $CLICKHOUSE_LOCAL -q "
    SELECT 'legacy_v1 $f';
    SELECT * FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json') ORDER BY id;
    SELECT id, amount FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json') WHERE category = 'alpha' ORDER BY id;
    SELECT id, amount FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json') WHERE category = 'beta';
    "
done

# Only some fields of a partition spec have a field id, so the missing ones can't be assigned by position.
# The v1 manifest has no `partition-spec-id`, and its own `partition-spec` (bucket[4]) does not match the
# partition spec of the table (identity).
for f in some-partition-field-ids no-matching-partition-spec
do
    echo "legacy_v1 $f $(rejected "SELECT * FROM icebergLocal('$LEGACY', SETTINGS iceberg_metadata_file_path = 'metadata/$f.metadata.json')")"
done

MERCH='data/persistent/iceberg_v1_repro/repro/merch_v1'

# A v1 table with a manifest entry of the status EXISTING without a sequence number, after deletes and a rewrite.
$CLICKHOUSE_LOCAL -q "
SELECT 'merch_v1';
SELECT * FROM icebergLocal('$MERCH') ORDER BY id;
SELECT * FROM icebergLocal('$MERCH', SETTINGS iceberg_metadata_file_path = 'metadata/00003-8d01e4aa-d143-49c9-898e-b5e477577b70.metadata.json') ORDER BY id;
"

# The table upgraded to v3 without a new snapshot: the v3 metadata references a v1 manifest list,
# where all the optional counts are NULL.
$CLICKHOUSE_LOCAL -q "
SELECT 'merch_v1 00004-v3-upgraded-v1-null-counts';
SELECT * FROM icebergLocal('$MERCH', SETTINGS iceberg_metadata_file_path = 'metadata/00004-v3-upgraded-v1-null-counts.metadata.json') ORDER BY id;
"

# A v3 table where no snapshot has a `schema-id`, which is optional in all format versions.
# Bugs metadata-file-names and optional-schema-id.
$CLICKHOUSE_LOCAL -q "
SELECT 'merch_v1 v3-no-snapshot-schema-id';
SELECT * FROM icebergLocal('$MERCH', SETTINGS iceberg_metadata_file_path = 'metadata/v3-no-snapshot-schema-id.metadata.json') ORDER BY id;
"

# Row lineage of a table upgraded from v2 to v3. Its first data file stores neither `_row_id` nor
# `_last_updated_sequence_number`, only one of them, or both (ids 1 to 4).
# Right after the upgrade, the data file has no `first_row_id`: both columns are NULL, also where the file stores a value.
# The first v3 snapshot assigns `first_row_id` 0 to the data file: the stored values are read as they are, and the
# missing ones are inherited (`first_row_id` + position, and the data sequence number 1).
# Time travel to the snapshot before the upgrade: no `first_row_id` again.
# Bug https://github.com/ClickHouse/ClickHouse/issues/130021: the values stored in the data file are returned when there
# is no `first_row_id`.
LINEAGE='data/persistent/row_lineage_stored_columns/warehouse/default/row_lineage_stored_columns'
$CLICKHOUSE_LOCAL -q "
SELECT 'row_lineage_stored_columns';
SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('$LINEAGE', SETTINGS iceberg_metadata_file_path = 'metadata/00002-upgraded.metadata.json') ORDER BY id;
SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('$LINEAGE') ORDER BY id;
SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('$LINEAGE') ORDER BY id SETTINGS iceberg_snapshot_id = 1111111111111111111;
"
