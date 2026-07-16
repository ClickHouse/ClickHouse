#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

(
  $CH_PATH client -mn -q "
  select version();
"
result=$($CH_PATH client -mn -q "
select toDateTimeOrNull('1965-05-02 02:00:00')
"
)

echo $result

if [ "$result" = "\N" ]; then
  exit 1
else
  exit 0
fi

)
