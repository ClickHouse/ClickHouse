#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# Old Iceberg format version 1 tables from the Dremio test suite,
# see `data_iceberg/dremio/README.md`.
# - The table schema is the legacy `schema` field without `schema-id`, and old snapshots have no `schema-id`.
# - `location` is `file:///tmp/iceberg` or `/tmp/iceberg`, while the manifest lists and data files are
#   referenced as `/tmp/iceberg/...`, `file:/tmp/iceberg/...` and `file:///tmp/iceberg/...`.
# - `partitionednation` is partitioned by identity of a column that was renamed later; its data files have no field ids.
# Expected values: the Dremio tests (`TestIcebergScan`: 25 rows in `nation`, 0 rows in `empty_table`) and the data files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/dremio"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}


# A table without snapshots. `v1.gc_disabled.metadata.json` is another version of the first metadata file
# with a single column, used by Dremio to test the `nessie.gc.enabled` property. Both files have version 1,
# so `v1.metadata.json` is selected explicitly: otherwise the choice would depend on the order of the directory listing.
echo empty_table
q "DESCRIBE icebergLocal('$DATA/empty_table', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json') SETTINGS describe_compact_output = 1"
q "SELECT count() FROM icebergLocal('$DATA/empty_table', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
q "SELECT * FROM icebergLocal('$DATA/empty_table', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
echo nation
q "DESCRIBE icebergLocal('$DATA/nation') SETTINGS describe_compact_output = 1"
q "SELECT count() FROM icebergLocal('$DATA/nation')"
q "SELECT * FROM icebergLocal('$DATA/nation') ORDER BY ALL"

# The first version of the table has no snapshots.
q "SELECT count() FROM icebergLocal('$DATA/nation', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
echo partitionednation
q "DESCRIBE icebergLocal('$DATA/partitionednation') SETTINGS describe_compact_output = 1"
q "SELECT * FROM icebergLocal('$DATA/partitionednation') ORDER BY ALL"
q "SELECT regionkey, count() FROM icebergLocal('$DATA/partitionednation') GROUP BY regionkey ORDER BY ALL"
q "SELECT n_nationkey, n_name FROM icebergLocal('$DATA/partitionednation') WHERE regionkey = 11"

# Older versions of the table: before the rename of `n_regionkey` and before the inserts of the two `MARTIAN-1` rows.
q "SELECT n_regionkey, count(), sum(n_nationkey) FROM icebergLocal('$DATA/partitionednation', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json') GROUP BY n_regionkey ORDER BY ALL"
q "SELECT regionkey, count(), sum(n_nationkey) FROM icebergLocal('$DATA/partitionednation', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json') GROUP BY regionkey ORDER BY ALL"
q "SELECT regionkey, count(), sum(n_nationkey) FROM icebergLocal('$DATA/partitionednation', SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json') GROUP BY regionkey ORDER BY ALL"
