#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Row-level deletes in Iceberg tables from lithium-tech/iceberg-cxx and lithium-tech/tea, see
# `data_iceberg/iceberg_cxx/README.md` and `data_iceberg/tea/README.md`. The expected rows follow from
# the `INSERT` and `DELETE` statements in the `create.sql` file next to each table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local allows `icebergLocal` only for paths inside the current directory.
cd "$CUR_DIR/data_iceberg" || exit 1

$CLICKHOUSE_LOCAL -q "
-- Trino, format version 3: deletion vectors after two DELETE statements (c1 % 3 = 0 on the first insert,
-- c1 % 3 = 1 on both inserts).
SELECT 'iceberg_cxx/deletion_vectors';
SELECT * FROM icebergLocal('iceberg_cxx/deletion_vectors') ORDER BY ALL;
-- Older snapshots: after the first insert, after the first DELETE, after the second insert.
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/deletion_vectors') ORDER BY c1) SETTINGS iceberg_snapshot_id = 9085912707585491401;
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/deletion_vectors') ORDER BY c1) SETTINGS iceberg_snapshot_id = 3380748922981262035;
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/deletion_vectors') ORDER BY c1) SETTINGS iceberg_snapshot_id = 7026943662205930455;

-- Trino: position deletes, then the data files are rewritten by OPTIMIZE, so the remaining position delete
-- files refer to data files that are no longer in the table (dangling deletes).
SELECT 'iceberg_cxx/dangling_deletes';
SELECT * FROM icebergLocal('iceberg_cxx/dangling_deletes') ORDER BY ALL;
SELECT count() FROM icebergLocal('iceberg_cxx/dangling_deletes') WHERE c1 % 3 = 0;
-- Older snapshots: after both inserts, after the DELETE, after each of the two partial OPTIMIZE runs.
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/dangling_deletes') ORDER BY c1) SETTINGS iceberg_snapshot_id = 583117362677106030;
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/dangling_deletes') ORDER BY c1) SETTINGS iceberg_snapshot_id = 6848200275181997717;
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/dangling_deletes') ORDER BY c1) SETTINGS iceberg_snapshot_id = 6262328802963518289;
SELECT groupArray(c1) FROM (SELECT c1 FROM icebergLocal('iceberg_cxx/dangling_deletes') ORDER BY c1) SETTINGS iceberg_snapshot_id = 171057654946844877;

-- Trino: two inserts and a DELETE of c1 % 3 = 0 that writes one position delete file per data file.
SELECT 'iceberg_cxx/granular_deletes';
SELECT * FROM icebergLocal('iceberg_cxx/granular_deletes') ORDER BY ALL;

-- Spark: 'write.delete.granularity' = 'file' with tiny target files, six data files with one position
-- delete file each.
SELECT 'iceberg_cxx/prod/db/refdeletes3';
SELECT * FROM icebergLocal('iceberg_cxx/prod/db/refdeletes3') ORDER BY ALL;

-- Trino: an UPDATE of all 7 rows that sets a column to NULL (7 position deletes and 7 rewritten rows).
SELECT 'tea/qa_etl_table_update_null';
SELECT * FROM icebergLocal('tea/qa_etl_table_update_null') ORDER BY ALL;
"
