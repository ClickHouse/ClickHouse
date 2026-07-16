#!/bin/bash
set -e

echo "$PWD"

$CH_PATH client -q "SELECT version()";

cat << EOF > /tmp/query.sql
drop table if exists tab;
create table tab (a String, b String, c String, d String) engine=MergeTree order by (a, b, c, d) ;

insert into tab select intDiv(number, 1e7), intDiv(number, 1e7), 0, number % 1000000 from numbers_mt(1e7 + 10);

select count() from tab where a = '0' and b = '0' and (c, d) in ('0', '12345');

EOF

result=$($CH_PATH client --queries-file /tmp/query.sql)
echo $result

result=$(echo "$result" | xargs)
# Check if the 'result' variable is not numerically equal to 10.
# We use -ne for a numerical "not equal" comparison.
if [[ "$result" -ne 10 ]]; then
  # If the condition is true, print an error message to standard error (>&2).
  echo "Error: Expected result 10, but got '$result'." >&2

  # Exit the script with a status code of 1 to indicate failure.
  exit 1
fi
