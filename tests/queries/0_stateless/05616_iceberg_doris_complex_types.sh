#!/usr/bin/env bash
# Tags: no-fasttest

# Deeply nested arrays, maps and structs in Iceberg tables from the Apache Doris tests,
# see `data_iceberg/doris/README.md`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/iceberg_complex_type.out`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# Four copies of the same table (ORC and Parquet, format versions 1 and 2), written by Spark:
#     id    int,
#     col2  array<array<array<array<array<int>>>>>,
#     col3  map<array<float>,map<int,map<int,float>>>,
#     col4  struct<x:array<int>,y:array<double>,z:map<boolean,string>>,
#     col5  map<int,map<int,map<int,map<int,map<float,map<double,struct<x:int,y:array<double>>>>>>>>,
#     col6  struct<xx:array<int>,yy:array<map<double,float>>,zz:struct<xxx:struct<xxxx:struct<xxxxx:decimal(13,2)>>>>
# The map key of `col3` is a list. In the Parquet files it is a LIST group inside the key of a MAP group,
# which must be read as `Array(Nullable(Float32))` (https://github.com/ClickHouse/ClickHouse/issues/130000).
for table in complex_orc_v1 complex_orc_v2 complex_parquet_v1 complex_parquet_v2
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        DESCRIBE icebergLocal('warehouse/wh/multi_catalog/$table') SETTINGS print_pretty_type_names = 0;
        SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY id;
        SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/$table');
        SELECT length(col2) FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY id;
        SELECT mapKeys(col3) FROM icebergLocal('warehouse/wh/multi_catalog/$table') ORDER BY id;
        SELECT col4.x[1] FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE id >= 7 ORDER BY id;
        SELECT id, count(col2) FROM icebergLocal('warehouse/wh/multi_catalog/$table') GROUP BY id ORDER BY id DESC LIMIT 2;
        SELECT id, col6.zz.xxx.xxxx.xxxxx, col5[2][2][2][2] FROM icebergLocal('warehouse/wh/multi_catalog/$table') WHERE col4.z[true] = 'example' ORDER BY id;
    " 2>&1
done

# Tables from the Doris BE unit tests (`be/test/exec/test_data`), written by Spark into HDFS
# (`hdfs://hdfs-cluster/user/hive/warehouse/cqtest.db/...`). The table location of
# `complex_user_profiles_iceberg_parquet` is `.../complex_user_profiles_iceberg` (the table was renamed).
# Doris has no expected values for these tables; the reference is checked against PyIceberg,
# and the ORC and Parquet versions give the same result.
for table in complex_user_profiles_iceberg_orc complex_user_profiles_iceberg_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('be/$table') ORDER BY id;
        SELECT name, profile.address.coordinates.lat, profile.address.coordinates.lng, profile.contact.email
        FROM icebergLocal('be/$table') WHERE id IN (3, 7) ORDER BY id;
        SELECT id, arrayMap(x -> x.name, profile.hobbies) FROM icebergLocal('be/$table') WHERE length(profile.hobbies) > 2 ORDER BY id;
        -- A subcolumn of an array of structs. In the Parquet table it currently reads as empty arrays
        -- (https://github.com/ClickHouse/ClickHouse/issues/130023), while \`arrayMap\` above and the ORC table are correct.
        SELECT id, profile.hobbies.name, profile.hobbies.level FROM icebergLocal('be/$table') WHERE id <= 3 ORDER BY id;
    " 2>&1
done

for table in nested_user_profiles_iceberg_orc nested_user_profiles_iceberg_parquet
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        DESCRIBE icebergLocal('be/$table') SETTINGS print_pretty_type_names = 0;
        SELECT * FROM icebergLocal('be/$table') ORDER BY id;
        -- \`friends.nickname\` and \`profile.hobbies.name\` are subcolumns of arrays of structs, see above.
        SELECT name, tags, friends.nickname, profile.hobbies.name, attributes['character'] FROM icebergLocal('be/$table') ORDER BY id;
        SELECT complex_attributes['premium_feature'].metadata FROM icebergLocal('be/$table') ORDER BY id;
    " 2>&1
done
