#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs pyarrow and IcebergLocal.
#
# BigQuery writes a private column `_CHANGE_TIMESTAMP` (field id 2147483345) into the data files
# of its managed Iceberg tables but keeps it out of the table schema. The id is below the range the
# spec reserves (above 2147483447) and above `last-column-id`. A reader projects by field id, so
# the column is simply not selected, and reading the table works.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_bigquery"
rm -rf "${TABLE_DIR}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_bigquery (x Int32) ENGINE = IcebergLocal('${TABLE_DIR}/');
    INSERT INTO t_bigquery VALUES (1), (2);
"

# Rewrite the data file the way BigQuery lays it out: the hidden column comes first.
python3 - "$(ls "${TABLE_DIR}"/data/*.parquet | head -1)" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

hidden = pa.field("_CHANGE_TIMESTAMP", pa.int64(), metadata={b"PARQUET:field_id": b"2147483345"})
x = pa.field("x", pa.int32(), nullable=False, metadata={b"PARQUET:field_id": b"1"})
table = pa.Table.from_arrays(
    [pa.array([100, 200], pa.int64()), pa.array([1, 2], pa.int32())],
    schema=pa.schema([hidden, x]),
)
pq.write_table(table, sys.argv[1])
PY

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_bigquery ORDER BY x;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_bigquery;"
rm -rf "${TABLE_DIR}"
