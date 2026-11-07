#!/usr/bin/env bash
# Tags: no-fasttest

# Equality and position deletes in Iceberg tables from the Apache Doris tests, see `data_iceberg/doris/README.md`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/test_iceberg_equality_delete.out`,
# `test_iceberg_position_delete.out` and `test_iceberg_read_with_posdelete.out`.
# Position deletes against ORC data files currently fail with "Position deletes are only supported
# for data files of Parquet format in Iceberg" (bug orc-avro-position-deletes): the `_orc` tables and
# `iceberg_position_gen_data` (ORC) must give the same results as the Parquet tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# Flink upsert tables (`'upsert-enabled'='true'`) with a primary key of one column (`id`) or three columns (`id1, id2, id3`):
#     insert into customer_flink_one values (1,'a');
#     insert into customer_flink_one values (2,'b');
#     insert into customer_flink_one values (1,'b');
#     insert into customer_flink_one values (2, 'a'),(2, 'c');
# Each upsert writes an equality delete file. The last insert also writes a position delete file,
# because the first row with the key 2 is replaced in the same commit.
for table in customer_flink_one customer_flink_one_orc
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') ORDER BY id;
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT max(val) FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT id FROM icebergLocal('warehouse/wh/test_db/$table') WHERE val = 'a';
    " 2>&1
done
for table in customer_flink_three customer_flink_three_orc
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') ORDER BY id1;
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT max(val) FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT id1 FROM icebergLocal('warehouse/wh/test_db/$table') WHERE val = 'a';
    " 2>&1
done

# Spark merge-on-read tables where rows are deleted with position delete files over several commits;
# 5632 rows remain (512 rows for each of 11 ids). `iceberg_position_gen_data` has the same data,
# written with 14 smaller commits.
for table in iceberg_position_parquet iceberg_position_orc iceberg_position_gen_data
do
    echo "$table"
    $CLICKHOUSE_LOCAL -q "
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE name = 'xyzxxxxxx' AND id != 9;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 1;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 5;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 10;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 15;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 2 ORDER BY ALL LIMIT 3;
        SELECT id, count(name) FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id != 1 GROUP BY id ORDER BY id;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 15 AND name = 'select xxxxxxxxx';
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 2 AND name = 'select xxxxxxxxx' ORDER BY ALL LIMIT 3;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id = 7 AND name = '12345xxx' ORDER BY ALL LIMIT 3;
        SELECT * FROM icebergLocal('warehouse/wh/test_db/$table') WHERE name = 'hello world';
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table') WHERE name != 'final entryxxxxxx';
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table');
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id != 2;
        SELECT count() FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id != 5;
        SELECT count(name) FROM icebergLocal('warehouse/wh/test_db/$table') WHERE id != 1 AND name != 'hello';
    " 2>&1
done

# A table with the `s3://` scheme in the location (the other tables use `s3a://`) and a position delete.
echo "location_s3_table"
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM icebergLocal('warehouse/wh/test_db/location_s3_table') ORDER BY id;
" 2>&1
