#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('Parquet' 'Arrow' 'ORC')

for format in "${FORMATS[@]}"
do
    echo $format
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 00163_column_oriented SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE 00163_column_oriented(ClientEventTime DateTime, MobilePhoneModel String, ClientIP6 FixedString(16)) ENGINE=File($format)"
    $CLICKHOUSE_CLIENT -q "INSERT INTO 00163_column_oriented SELECT ClientEventTime, MobilePhoneModel, ClientIP6 FROM test.hits ORDER BY ClientEventTime, MobilePhoneModel, ClientIP6 LIMIT 100"
    $CLICKHOUSE_CLIENT -q "SELECT ClientEventTime from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT MobilePhoneModel from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "SELECT ClientIP6 from 00163_column_oriented" | md5sum
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 00163_column_oriented SYNC"
done
