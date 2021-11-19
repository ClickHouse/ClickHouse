#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo $CUR_DIR
parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_parallel"
non_parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_non_parallel"


echo -ne '{prefix} \n${data}\n {suffix}' > $CUR_DIR/02122_template_format_resultset.tmp
echo -ne 'x:${x:Quoted}, y:${y:Quoted}, s:${s:Quoted}' > $CUR_DIR/02122_template_format_row.tmp

TEMPLATE_SETTINGS="SETTINGS format_template_resultset = '$CUR_DIR/02122_template_format_resultset.tmp', format_template_row = '$CUR_DIR/02122_template_format_row.tmp', format_template_rows_between_delimiter = ';\n'"

echo "Template"
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(500000) format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=0 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(500000) format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=1 > $parallel_file

diff "$non_parallel_file"_1 "$parallel_file"_1 > diff1
wc -l diff1

echo -ne '{prefix} \n${data}\n $$ suffix $$\n${totals}\n${min}\n${max}\n${rows:Quoted}\n${rows_before_limit:Quoted}\n${rows_read:Quoted}\n${bytes_read:Quoted}\n' > $CUR_DIR/02122_template_format_resultset.tmp
echo -ne 'x:${x:Quoted}, y:${y:Quoted}, s:${s:Quoted}' > $CUR_DIR/02122_template_format_row.tmp

$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(500000) group by x with totals limit 490000 format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=0 --extremes=1 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(500000) group by x with totals limit 490000 format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=1 --extremes=1 > $parallel_file

diff $non_parallel_file $parallel_file > diff
wc -l diff

rm $CUR_DIR/02122_template_format_resultset.tmp $CUR_DIR/02122_template_format_row.tmp

