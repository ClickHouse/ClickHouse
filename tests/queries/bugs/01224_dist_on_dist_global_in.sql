create table if not exists data_01224 (key Int) Engine=Memory();
create table if not exists dist_layer_01224 as data_01224 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01224);
create table if not exists dist_01224 as data_01224 Engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01224);

select * from dist_01224;
insert into data_01224 select * from numbers(3);

-- "Table expression is undefined, Method: ExpressionAnalyzer::interpretSubquery"
select 'GLOBAL IN distributed_group_by_no_merge';
select distinct * from dist_01224 where key global in (1) settings distributed_group_by_no_merge=1;

-- requires #9923
select 'GLOBAL IN';
select distinct * from dist_01224 where key global in (1);

drop table dist_01224;
drop table dist_layer_01224;
drop table data_01224;
