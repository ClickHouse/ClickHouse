#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

(
  $CH_PATH client -mn -q "
  select version();
  drop table if exists tbl;
create table tbl (
  timestamp DateTime,
  projection proj (select * order by timestamp))
engine MergeTree order by ()
  TTL timestamp + INTERVAL 1 DAY
settings min_bytes_for_wide_part = 1, index_granularity = 1;

insert into tbl
select today() - 100
union all
select today() + 50;

optimize table tbl final;


"
result=$($CH_PATH client -mn -q "
SELECT groupBitXor(cityHash64(*))
FROM (
  select rows from (
    select rows from system.parts where table = 'tbl' and active
    union all
    select rows from system.projection_parts where table = 'tbl' and active
  )
  order by rows
);
"
)

echo $result

if [ "$result" = "0" ]; then
  exit 1
else
  exit 0
fi

)
