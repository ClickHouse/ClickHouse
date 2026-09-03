#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()";

$CH_PATH client -mn -q "
drop table if exists t1;
drop table if exists t2;

CREATE TABLE t1
(
    c1 UInt64,
    c2 UInt64,
    c3 Nullable(UInt64)
)
engine = MergeTree()
PRIMARY KEY (c1, c2)
ORDER BY (c1, c2);

CREATE TABLE t2
(
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
)
engine = MergeTree()
PRIMARY KEY (c1, c2)
ORDER BY (c1, c2);


insert into t1 select * from generateRandom() limit 10000;
insert into t2 select * from generateRandom() limit 10000;

SELECT
    count()
FROM t1
INNER JOIN t2 ON t1.c3 = t2.c3
WHERE
     has([], t1.c3)

"

