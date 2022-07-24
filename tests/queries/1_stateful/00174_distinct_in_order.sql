select '-- check that distinct with and w/o optimization produce the same result';

drop table if exists distinct_in_order sync;
drop table if exists ordinary_distinct sync;

select '-- DISTINCT colums are the same as in ORDER BY';
create table distinct_in_order (CounterID UInt32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into distinct_in_order select distinct CounterID, EventDate from hits_v1 order by CounterID, EventDate settings optimize_distinct_in_order=1;
create table ordinary_distinct (CounterID UInt32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into ordinary_distinct select distinct CounterID, EventDate from hits_v1 order by CounterID, EventDate settings optimize_distinct_in_order=0;
select distinct * from distinct_in_order except select * from ordinary_distinct;

drop table if exists distinct_in_order sync;
drop table if exists ordinary_distinct sync;

select '-- DISTINCT colums has prefix in ORDER BY columns';
create table distinct_in_order (CounterID UInt32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into distinct_in_order select distinct CounterID, EventDate from hits_v1 order by CounterID settings optimize_distinct_in_order=1;
create table ordinary_distinct (CounterID UInt32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into ordinary_distinct select distinct CounterID, EventDate from hits_v1 order by CounterID settings optimize_distinct_in_order=0;
select distinct * from distinct_in_order except select * from ordinary_distinct;

drop table if exists distinct_in_order sync;
drop table if exists ordinary_distinct sync;

