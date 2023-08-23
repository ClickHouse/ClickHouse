#!/usr/bin/env bash
# Tags: disabled

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parsing_bson"
$CLICKHOUSE_CLIENT -q "CREATE TABLE parsing_bson(WatchID UInt64, ClientIP6 FixedString(16), EventTime DateTime, Title String) ENGINE=Memory()"


$CLICKHOUSE_CLIENT --max_threads=0 --max_block_size=65505 --output_format_parallel_formatting=false -q \
"SELECT WatchID, ClientIP6, EventTime, Title FROM test.hits ORDER BY UserID LIMIT 30000 Format BSONEachRow" > 00176_data.bson

cat 00176_data.bson | $CLICKHOUSE_CLIENT --max_threads=0 --input_format_parallel_parsing=false -q "INSERT INTO parsing_bson FORMAT BSONEachRow"

checksum1=$($CLICKHOUSE_CLIENT -q "SELECT * FROM parsing_bson ORDER BY WatchID;" | md5sum)
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE parsing_bson;"

cat 00176_data.bson | $CLICKHOUSE_CLIENT --max_threads=0 --max_insert_block_size=5000 --input_format_parallel_parsing=true -q "INSERT INTO parsing_bson FORMAT BSONEachRow"

checksum2=$($CLICKHOUSE_CLIENT -q "SELECT * FROM parsing_bson ORDER BY WatchID;" | md5sum)


if [[ "$checksum1" == "$checksum2" ]];
then
    echo "OK"
else
    echo "FAIL"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE parsing_bson"

rm 00176_data.bson

