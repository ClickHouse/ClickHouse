#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()";

$CH_PATH client -mn -q "
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

set max_partitions_per_insert_block=99999999;
set compatibility='23.3';

CREATE TABLE t1
(
    LeadId String,
    ProductId String,
    CreatedDate DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(CreatedDate)
ORDER BY LeadId;

CREATE TABLE t2
(
    lookup_id Int64
)
ENGINE = MergeTree()
ORDER BY lookup_id;


CREATE TABLE t3
(
    InteractionId String,
    CreatedDate DateTime,
    IneractionStatus String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(CreatedDate)
ORDER BY InteractionId;


insert into t1 select * from generateRandom() limit 999;
insert into t2 select * from generateRandom() limit 999;
insert into t3 select * from generateRandom() limit 999;

SELECT
    count()
FROM t1 AS lead
INNER JOIN t2 AS lo ON toInt64OrNull(lead.ProductId) = lo.lookup_id
INNER JOIN t3 AS i ON toInt64OrNull(i.IneractionStatus) = lo.lookup_id
FORMAT NULL;
"
