#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('JSONEachRow' 'JSONCompactEachRow' 'JSONCompactStringsEachRow' 'JSONCompactEachRowWithNames' 'JSONCompactStringsEachRowWithNames' 'JSONCompactEachRowWithNamesAndTypes' 'JSONCompactStringsEachRowWithNamesAndTypes')

for format in "${FORMATS[@]}"
do
    echo "$format, false";
    $CLICKHOUSE_CLIENT --max_threads=0 --output_format_parallel_formatting=false -q \
    "SELECT ClientEventTime::DateTime('Asia/Dubai') as a, MobilePhoneModel as b, ClientIP6 as c FROM test.hits ORDER BY a, b, c LIMIT 3000000 Format $format" | md5sum

    echo "$format, true";
    $CLICKHOUSE_CLIENT --max_threads=0 --output_format_parallel_formatting=true -q \
    "SELECT ClientEventTime::DateTime('Asia/Dubai') as a, MobilePhoneModel as b, ClientIP6 as c FROM test.hits ORDER BY a, b, c LIMIT 3000000 Format $format" | md5sum
done
