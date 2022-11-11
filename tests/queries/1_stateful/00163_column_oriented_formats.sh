#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('Parquet' 'Arrow' 'ORC')

for format in "${FORMATS[@]}"
do
    echo $format
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS column_oriented_00163 SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE column_oriented_00163(ClientEventTime DateTime('Asia/Dubai'), MobilePhoneModel String, ClientIP6 FixedString(16)) ENGINE=File($format)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO column_oriented_00163 SELECT ClientEventTime, MobilePhoneModel, ClientIP6 FROM test.hits ORDER BY ClientEventTime, MobilePhoneModel, ClientIP6 LIMIT 100"
    $CLICKHOUSE_CLIENT -q "SELECT ClientEventTime from column_oriented_00163" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT MobilePhoneModel from column_oriented_00163" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT ClientIP6 from column_oriented_00163" | md5sum
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS column_oriented_00163 SYNC"
done
