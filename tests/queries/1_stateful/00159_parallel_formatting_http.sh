#!/usr/bin/env bash
# Tags: no-tsan
# FIXME It became flaky after upgrading to llvm-14 due to obscure freezes in tsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('TSV' 'CSV' 'JSONCompactEachRow')

for format in "${FORMATS[@]}"
do
    echo "$format, false";
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+ClientEventTime::DateTime('Asia/Dubai')+as+a,MobilePhoneModel+as+b,ClientIP6+as+c+FROM+test.hits+ORDER+BY+a,b,c+LIMIT+1000000+Format+$format&output_format_parallel_formatting=false" -d' ' | md5sum

    echo "$format, true";
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+ClientEventTime::DateTime('Asia/Dubai')+as+a,MobilePhoneModel+as+b,ClientIP6+as+c+FROM+test.hits+ORDER+BY+a,b,c+LIMIT+1000000+Format+$format&output_format_parallel_formatting=true" -d' ' | md5sum
done
