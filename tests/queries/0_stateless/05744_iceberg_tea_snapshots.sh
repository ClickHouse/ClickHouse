#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Snapshot selection in Iceberg tables from lithium-tech/tea, see `data_iceberg/tea/README.md`.
# The expected results are from the upstream tests in `tea/smoke_test/iceberg_test.cpp` and the generation
# scripts (`create.sql`, `test/iceberg/gen/gperov_test.py`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local allows `icebergLocal` only for paths inside the current directory.
cd "$CUR_DIR/data_iceberg/tea" || exit 1

$CLICKHOUSE_LOCAL -q "
-- Spark: 10000 rows (i, 2 * i), DELETE of a = 2101 by a position delete, then two single-row inserts.
-- The insert of (-42, -42) was meant for a branch, but Spark wrote both inserts to main.
SELECT 'gperov/test';
DESCRIBE icebergLocal('gperov/test') SETTINGS describe_compact_output = 1;
SELECT count(), sum(a), sum(b), min(a), max(a) FROM icebergLocal('gperov/test');
SELECT * FROM icebergLocal('gperov/test') WHERE a < 0 OR a BETWEEN 2100 AND 2102 ORDER BY ALL;
SELECT count() FROM icebergLocal('gperov/test') WHERE a > 100000;
-- The snapshot after the DELETE, which the upstream tests read (9999 rows, 4999 with a > 5000), selected
-- by id and by its metadata file; then the next snapshot.
SELECT count(), sum(a), sum(b) FROM icebergLocal('gperov/test') SETTINGS iceberg_snapshot_id = 7558608030923099867;
SELECT count() FROM icebergLocal('gperov/test') WHERE a > 5000 SETTINGS iceberg_snapshot_id = 7558608030923099867;
SELECT count() FROM icebergLocal('gperov/test', SETTINGS iceberg_metadata_file_path = 'metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json');
SELECT count() FROM icebergLocal('gperov/test') SETTINGS iceberg_snapshot_id = 293191470325800019;

-- A table without snapshots.
SELECT 'empty/empty';
DESCRIBE icebergLocal('empty/empty') SETTINGS describe_compact_output = 1;
SELECT count() FROM icebergLocal('empty/empty');
SELECT * FROM icebergLocal('empty/empty');

-- Spark: (a, b) with 3 rows, branch new_branch, DROP COLUMN b, 3 more rows. The old snapshot (the head of
-- new_branch) is read with its own schema (a, b).
SELECT 'mydb/multiple_branches';
SELECT * FROM icebergLocal('mydb/multiple_branches') ORDER BY ALL;
SELECT * FROM icebergLocal('mydb/multiple_branches') ORDER BY ALL SETTINGS iceberg_snapshot_id = 7178038598552999887;
SELECT * FROM icebergLocal('mydb/multiple_branches') ORDER BY ALL SETTINGS iceberg_snapshot_id = 4741018521999590032;

-- Trino: an empty snapshot of CREATE TABLE, then 8 rows.
SELECT 'snapshot_selection';
SELECT * FROM icebergLocal('snapshot_selection') ORDER BY ALL;
SELECT count() FROM icebergLocal('snapshot_selection') SETTINGS iceberg_snapshot_id = 2425900280988415891;
SELECT count() FROM icebergLocal('snapshot_selection') SETTINGS iceberg_snapshot_id = 3335409763063846084;
"

# A snapshot id that does not exist.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM icebergLocal('snapshot_selection') SETTINGS iceberg_snapshot_id = 999999" 2>&1 | grep -c 'Code: 743'
