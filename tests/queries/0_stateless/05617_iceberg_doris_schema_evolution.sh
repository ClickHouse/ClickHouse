#!/usr/bin/env bash
# Tags: no-fasttest

# Schema evolution and column names in Iceberg tables from the Apache Doris tests,
# see `data_iceberg/doris/README.md`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/iceberg_schema_change.out`,
# `test_iceberg_upper_case_column_name.out` and `test_external_catalog_iceberg_common.out`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# Four copies of the same table (ORC and Parquet, format versions 1 and 2), written by Spark with this schema:
#     id int, col1 array<int>, col2 array<float>, col3 array<decimal(12,4)>, col4 map<int,int>, col5 map<int,float>,
#     col6 map<int,decimal(12,5)>, col7 struct<x:int,y:float,z:decimal(12,5)>, col8 int, col9 float, col10 decimal(12,5), col_del int
# Then the types are widened (int -> bigint, float -> double, decimal(12,N) -> decimal(20,N), also inside arrays, maps and structs),
# `col7.z` and `col_del` are dropped, `col7.add` is added as the first field of the struct, all columns are renamed,
# `rename_col8`, `rename_col9`, `rename_col10` are moved to the front, `col_add` and `col_add2` are added,
# and five more rows are inserted with the new schema.
# The arrays and maps of the rows with id 5 and 15 are NULL, which is read as empty arrays and maps.
# Currently any query to these tables fails with "Bad get: has Decimal64, requested Decimal128" in `EvolutionFunctionStruct`
# because of the widened decimal inside an array (https://github.com/ClickHouse/ClickHouse/issues/130024).
for table in complex_orc_v1_schema_change complex_orc_v2_schema_change complex_parquet_v1_schema_change complex_parquet_v2_schema_change
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        DESCRIBE icebergLocal('warehouse/wh/test_db/$table') SETTINGS print_pretty_type_names = 0;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') ORDER BY id;
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT id FROM icebergLocal('warehouse/wh/test_db/$table') WHERE col_add + 1 = col_add2 ORDER BY id;
        SELECT col_add FROM icebergLocal('warehouse/wh/test_db/$table') WHERE col_add IS NOT NULL ORDER BY col_add;
        SELECT id FROM icebergLocal('warehouse/wh/test_db/$table') WHERE col_add + 5 = id ORDER BY id;
        SELECT rename_col7 FROM icebergLocal('warehouse/wh/test_db/$table') ORDER BY id;
        SELECT col_add2 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id >= 7 ORDER BY id;
        SELECT length(rename_col3), rename_col3 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE length(rename_col3) > 2 ORDER BY id;
        SELECT length(rename_col2), rename_col2 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE rename_col2[1] > 7 AND id > 7 ORDER BY id;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE rename_col10 > 500 ORDER BY id;
        SELECT id, rename_col4 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE mapKeys(rename_col4)[1] > 10 ORDER BY id;
        SELECT id, rename_col5 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE mapValues(rename_col5)[1] > 10 ORDER BY id;
        SELECT rename_col7.add, rename_col7.x FROM icebergLocal('warehouse/wh/test_db/$table') ORDER BY id;
        SELECT id, rename_col8, rename_col9 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE rename_col8 + rename_col9 > 100 ORDER BY id;
        SELECT id, rename_col6 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id IN (4, 14) ORDER BY id;
    " 2>&1
done

# Upper case column names (`ID`, `NAME`), the table was written by Spark with eight metadata versions.
for table in iceberg_upper_case_orc iceberg_upper_case_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        DESCRIBE icebergLocal('warehouse/wh/test_db/$table');
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE ID = 1;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE ID > 1;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE NAME = 'name';
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE NAME != 'name';
        SELECT NAME FROM icebergLocal('warehouse/wh/test_db/$table') WHERE ID = 1;
    " 2>&1
done

# Column names with characters that are not valid in Avro names (`/DSD/SV_CNT_GRP`, `/SAPMP/KADP`),
# and an identity partition on `MATNR`.
echo "sanitize_mara"
$CLICKHOUSE_LOCAL -q "
    DESCRIBE icebergLocal('warehouse/wh/multi_catalog/sanitize_mara');
    SELECT MATNR, NTGEW, \`/DSD/SV_CNT_GRP\` FROM icebergLocal('warehouse/wh/multi_catalog/sanitize_mara') ORDER BY MATNR;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/sanitize_mara') WHERE \`/SAPMP/KADP\` > 5.3 ORDER BY ALL;
" 2>&1
