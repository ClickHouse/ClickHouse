-- Tags: distributed, no-parallel
-- no-parallel: uses the shared `shard_0` / `shard_1` databases of `test_cluster_two_shards_different_databases`.

-- A non-GLOBAL `IN (subquery)` written outside a subquery / view / CTE over a Distributed table is pushed down into the
-- query sent to the shards (`allow_push_predicate_ast_for_distributed_subqueries`). When the subquery of the IN reads a
-- Distributed table itself, `distributed_product_mode` has to apply to the pushed-down copy exactly as it applies to the
-- same IN written over the Distributed table directly. It used to be ignored: the subquery went to every shard as text,
-- naming the initiator's Distributed table, whatever the setting said.

-- The two "shards" are two databases of this server, so that they can hold different rows under one table name.
create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.events_05507;
drop table if exists shard_1.events_05507;
drop table if exists shard_0.lookup_05507;
drop table if exists shard_1.lookup_05507;
drop table if exists events_dist;
drop table if exists lookup_dist;
drop view if exists events_view;

create table shard_0.events_05507 (id UInt64) engine = MergeTree order by id;
create table shard_1.events_05507 (id UInt64) engine = MergeTree order by id;
create table shard_0.lookup_05507 (id UInt64) engine = MergeTree order by id;
create table shard_1.lookup_05507 (id UInt64) engine = MergeTree order by id;

-- Events 1..10 live on shard 0, 11..20 on shard 1. The lookup table is "crossed": each shard stores the ids whose
-- events live on the other shard. All 10 lookup ids have an event, but no shard finds any of them in its own lookup table.
insert into shard_0.events_05507 select number from numbers(1, 10);
insert into shard_1.events_05507 select number from numbers(11, 10);
insert into shard_0.lookup_05507 select number from numbers(11, 5);
insert into shard_1.lookup_05507 select number from numbers(1, 5);

create table events_dist (id UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', events_05507);
create table lookup_dist (id UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', lookup_05507);
create view events_view as select id from events_dist;

set enable_analyzer = 1;
-- The shards are this very server; make the initiator talk to them over the network as to real remote shards.
set prefer_localhost_replica = 0;
-- A shard must not run the subquery of the IN once more over the Distributed table: with `global` it receives the set,
-- with `local` it reads its own table. `max_distributed_depth = 1` turns such a second fan-out into an error.
set max_distributed_depth = 1;

select '-- global';
set distributed_product_mode = 'global';
select 'table', count() from events_dist where id in (select id from lookup_dist);
select 'subquery', count() from (select id from events_dist) where id in (select id from lookup_dist);
select 'view', count() from events_view where id in (select id from lookup_dist);
with e as (select id from events_dist) select 'cte', count() from e where id in (select id from lookup_dist);
select 'view, IN table', count() from events_view where id in lookup_dist;
select 'view, NOT IN', count() from events_view where id not in (select id from lookup_dist);
select 'view, with another conjunct', count() from events_view where id > 2 and id in (select id from lookup_dist);
select 'view, explicit GLOBAL IN', count() from events_view where id global in (select id from lookup_dist);
select 'view, explicit GLOBAL IN table', count() from events_view where id global in lookup_dist;

select '-- prefer_global_in_and_join';
set distributed_product_mode = 'deny', prefer_global_in_and_join = 1;
select 'view', count() from events_view where id in (select id from lookup_dist);
set prefer_global_in_and_join = 0;

select '-- local';
-- `local` makes each shard read its own lookup table, which here never has the ids of that shard's events.
set distributed_product_mode = 'local';
select 'table', count() from events_dist where id in (select id from lookup_dist);
select 'view', count() from events_view where id in (select id from lookup_dist);

-- `deny` refuses before anything is sent and `allow` fans out twice by definition, so the depth limit is back to its default.
set max_distributed_depth = 5;

select '-- deny';
set distributed_product_mode = 'deny';
select 'table', count() from events_dist where id in (select id from lookup_dist); -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
select 'subquery', count() from (select id from events_dist) where id in (select id from lookup_dist); -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
select 'view', count() from events_view where id in (select id from lookup_dist); -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
with e as (select id from events_dist) select 'cte', count() from e where id in (select id from lookup_dist); -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
-- Without the push-down there is no second distributed subquery, hence nothing to deny.
select 'view, no push-down', count() from events_view where id in (select id from lookup_dist) settings allow_push_predicate_ast_for_distributed_subqueries = 0;

select '-- allow';
-- `allow` sends the subquery as it is written, over the Distributed table, so the shards do fan out once more.
set distributed_product_mode = 'allow';
select 'table', count() from events_dist where id in (select id from lookup_dist);
select 'view', count() from events_view where id in (select id from lookup_dist);

drop view events_view;
drop table events_dist;
drop table lookup_dist;
drop table shard_0.events_05507;
drop table shard_1.events_05507;
drop table shard_0.lookup_05507;
drop table shard_1.lookup_05507;
