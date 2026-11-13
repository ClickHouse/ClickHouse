#!/usr/bin/env bash
# Tags: long, distributed, no-fasttest
# long: many queries; near the time limit of the flaky check under the thread fuzzer.
# no-fasttest: distributed index analysis

# What the shards are sent for an `IN` written over a subquery that reads a `Distributed` table: its text, or the
# result of the right side (`_data_`) if `distributed_product_mode` is 'global' where the `Distributed` table is read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
create table events (id UInt64, g UInt64) engine = MergeTree order by id;
insert into events select number, number % 4 from numbers(20);
create table some (id UInt64) engine = MergeTree order by id;
insert into some values (1), (2), (12), (100);
create table events_dist as events engine = Distributed(test_cluster_two_shards, currentDatabase(), events);
create table some_dist as some engine = Distributed(test_cluster_two_shards, currentDatabase(), some);
create table some_one_shard as some engine = Distributed(test_shard_localhost, currentDatabase(), some);
create view some_view as with c as (select id from some) select id from c;
create view numbers_view as select number as id from numbers(5);
create table some_set (id UInt64) engine = Set;
insert into some_set values (1), (2);
create view some_dist_view as select id from some_dist;
create view some_dist_view_view as select id from some_dist_view;
"

# name | condition | source
CHECKS=$(cat <<'EOF'
constants|id in (1, 2, 12)
constants, not in|id not in (1, 2, 12)
a tuple of constants|(id, g) in ((1, 1), (12, 0))
local table|id in (select id from some)
local table, deny|id in (select id from some) settings distributed_product_mode = 'deny'
local table, not in|id not in (select id from some)
local table, global in|id global in (select id from some)
temporary table|id in temp
Distributed, and a temporary table|id in temp and id + 1 in (select id from some_dist)
Distributed, and a temporary table, global|id global in temp and id + 1 in (select id from some_dist)
Distributed, and a temporary table in a subquery|id in (select id from temp) and id + 1 in (select id from some_dist)
local table in an or|g = 3 or id in (select id from some)
in table, local|id in some
view over a local table|id in (select id from some_view)
view over numbers()|id in (select id from numbers_view)
numbers()|id in (select number from numbers(5))
remote()|id in (select id from remote('127.0.0.{1,2}', currentDatabase(), some))
Distributed over one shard|id in (select id from some_one_shard)
Distributed, global in|id global in (select id from some_dist)
Distributed|id in (select id from some_dist)
Distributed, in a view|id in (select id from some_dist_view)
Distributed, in a view of a view|id in (select id from some_dist_view_view)
Distributed, in table|id in some_dist
Distributed, in merge()|id in (select id from merge(currentDatabase(), '^some_dist$'))
view()|id in (select number from view(select number from numbers(5)))
Distributed, deny|id in (select id from some_dist) settings distributed_product_mode = 'deny'
Distributed, deny, prefer global|id in (select id from some_dist) settings distributed_product_mode = 'deny', prefer_global_in_and_join = 1
Distributed, local|id in (select id from some_dist) settings distributed_product_mode = 'local'
Distributed, not in|id not in (select id from some_dist)
Distributed, in an or|g = 3 or id in (select id from some_dist)
Distributed, under a not|not (g != 3 and id in (select id from some_dist))
Distributed, and another conjunct|g != 3 and id in (select id from some_dist)
Distributed, and a local one|id in (select id from some) and id + 1 in (select id from some_dist)
Distributed, global and local|id global in (select id from some_dist) and id + 1 in (select id from some_dist)
Distributed, set built for a virtual column|p in (select toString(id) from some_dist)|(select toString(id) as p from events_dist union all select _partition_id from events)
Distributed, read to build another set first|1 settings query_plan_convert_join_to_in = 1, query_plan_join_swap_table = 'false', use_index_for_in_with_subqueries = 1|(select count() from some a join (select id from events_dist where g > 10) c on a.id = c.id and c.id in (select id from some_dist))
Distributed, allow|id in (select id from some_dist) settings distributed_product_mode = 'allow'
Distributed, allow, prefer global|id in (select id from some_dist) settings distributed_product_mode = 'allow', prefer_global_in_and_join = 1
Distributed, local, prefer global|id in (select id from some_dist) settings distributed_product_mode = 'local', prefer_global_in_and_join = 1
Distributed, allow in the right side|id in (select id from some_dist settings distributed_product_mode = 'allow')
Distributed, global in the right side only|id in (select id from some_dist settings distributed_product_mode = 'global') settings distributed_product_mode = 'allow'
Distributed, global in the left side only|id in (select id from some_dist) settings distributed_product_mode = 'allow'|(select id, g from events_dist settings distributed_product_mode = 'global')
Distributed, allow in the left side only|id in (select id from some_dist)|(select id, g from events_dist settings distributed_product_mode = 'allow')
Distributed, the left side has a global in|id in (select id from some_dist)|(select id, g from events_dist where g global in (select id from some))
Set|id in some_set
Distributed, and global in constants|id global in (1, 2) and id + 1 in (select id from some_dist)
Distributed, and one that is not deterministic|id + rand() % 2 in (select id from some_dist) and id in (select id from some_dist_view)
Distributed, the same set is global elsewhere|id in (select id from some_dist) having sum(id global in (select id from some_dist)) > 0
Distributed, and an in without a set|inIgnoreSet(id, (1, 2)) = 0 and id in (select id from some_dist)
Distributed, and a Set|id in some_set and id + 1 in (select id from some_dist)
Distributed, and a constant|id in (1, 2) and id + 1 in (select id from some_dist)
Distributed, and one over one shard|id in (select id from some_dist) and id + 1 in (select id from some_one_shard)
Distributed, or one that is allowed|id in (select id from some_dist) or id + 1 in (select id from some_dist settings distributed_product_mode = 'allow')
Distributed, all is limited|id in (select id from some_dist) settings max_rows_in_set = 100, max_rows_to_transfer = 100, max_rows_to_read = 1000000, max_execution_time = 1000
Distributed, set may be cut|id in (select id from some_dist) settings max_rows_in_set = 100, set_overflow_mode = 'break'
Distributed, set is cut, not on the shards|id in (select id from some_dist) settings max_rows_in_set = 2, set_overflow_mode = 'break'|(select id from events_dist settings max_rows_in_set = 0, set_overflow_mode = 'throw' union all select id from some)
Distributed, set may be cut on the shards|id in (select id from some_dist)|(select id, g from events_dist settings max_rows_in_set = 100, set_overflow_mode = 'break')
Distributed, table may be cut|id in (select id from some_dist) settings max_rows_to_transfer = 100, transfer_overflow_mode = 'break'
Distributed, table may not be cut, the set is built|id in (select id from some_dist) settings max_rows_to_transfer = 4, max_bytes_to_transfer = 32, transfer_overflow_mode = 'break'|(select id from events_dist union all select id from some)
Distributed, the set is built, with too many rows|id in (select id from some_dist) settings max_rows_to_transfer = 3|(select id from events_dist union all select id from some)
Distributed, the set is built, with too many bytes|id in (select id from some_dist) settings max_bytes_to_transfer = 31|(select id from events_dist union all select id from some)
Distributed, the set is built, with as many bytes of other types|id in (select toNullable(id) from some_dist) settings max_bytes_to_transfer = 36|(select id from events_dist union all select id from some)
Distributed, the set is built, with too many bytes of other types|id in (select toNullable(id) from some_dist) settings max_bytes_to_transfer = 35|(select id from events_dist union all select id from some)
Distributed, reading may be cut|id in (select id from some_dist) settings max_rows_to_read = 1000000, read_overflow_mode = 'break'
Distributed, reading may be cut on the shards|id in (select id from some_dist) settings max_rows_to_read_leaf = 1000000, read_overflow_mode_leaf = 'break'
Distributed, time may be cut|id in (select id from some_dist) settings max_execution_time = 1000, timeout_overflow_mode = 'break'
Distributed, time may be cut only here|id in (select id from some_dist) settings max_execution_time = 1000, timeout_overflow_mode = 'break', max_execution_time_leaf = 1000
Distributed, time may be cut on the shards|id in (select id from some_dist) settings max_execution_time_leaf = 1000, timeout_overflow_mode_leaf = 'break'
Distributed, other nulls on the shards|id in (select id from some_dist)|(select id, g from events_dist settings transform_null_in = 1)
Distributed, custom key|id in (select id from some_dist) settings enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'id', parallel_replicas_for_non_replicated_merge_tree = 1
Distributed, parallel replicas|id in (select id from some_dist) settings enable_parallel_replicas = 1, max_parallel_replicas = 2, automatic_parallel_replicas_mode = 0
Distributed, parallel replicas without in|id in (select id from some_dist) settings enable_parallel_replicas = 1, max_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_in_with_subquery = 0
Distributed, parallel replicas without in, not pushed down|id in (select id from some_dist) settings enable_parallel_replicas = 1, max_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_filter_pushdown = 0
Distributed, no parallel replicas, without in|id in (select id from some_dist) settings parallel_replicas_allow_in_with_subquery = 0
global inside, allow|id in (select number from numbers(5) where number global in (select id from some)) settings distributed_product_mode = 'allow'
EOF
)

CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1 --prefer_localhost_replica 0 --enable_parallel_replicas 0 --serialize_query_plan 0 --enable_add_distinct_to_in_subqueries 0"
LOG="event_date >= yesterday() and event_time >= now() - 600 and startsWith(log_comment, '05529_$CLICKHOUSE_DATABASE|')"

# `serialize_query_plan = 0`: a plan is sent without what is pushed down.
i=100
(
    echo "create temporary table temp (id UInt64); insert into temp select 1;"
    while IFS='|' read -r name condition source
    do
        echo "set log_comment = '05529_$CLICKHOUSE_DATABASE|$((i++))|$name';"
        echo "select count() from ${source:-(select id, g from events_dist)} where $condition format Null;"
    done <<< "$CHECKS"
) | $CLIENT --distributed_product_mode global

$CLIENT -q "
system flush logs query_log;
select splitByChar('|', log_comment)[3], arrayStringConcat(groupUniqArray(
    extract(replaceRegexpAll(replaceAll(query, currentDatabase(), 'db'), '_data_[0-9a-f_-]+', '_data_'), 'HAVING (.*)')), ' ; ')
from system.query_log
where $LOG and not is_initial_query and type = 'QueryStart' and has(tables, currentDatabase() || '.events')
group by log_comment order by log_comment"

# The set of two `IN`s is sent once, and not for nothing. The shards prune by it, and read other columns only where it matches.
# Replicas that read the data of this server, or analyze its indexes, stand in for it and get the text. Many parts: some
# of them have to be given to the other replica.
$CLIENT --distributed_product_mode global --network_compression_method none -q "
create table parts (id UInt64) engine = MergeTree order by id
    settings merge_selector_base = 1000, index_granularity = 128, index_granularity_bytes = 10e9, distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;
system stop merges parts;
insert into parts select number from numbers(10000) settings max_block_size = 500, min_insert_block_size_rows = 500, min_insert_block_size_bytes = 0, max_insert_threads = 1, async_insert = 0;

set log_comment = '05529_$CLICKHOUSE_DATABASE|1|sent';
select count() from (select id from events_dist) where id in (select number from numbers(100000)) or id + 1 in (select number from numbers(100000)) format Null;
set log_comment = '05529_$CLICKHOUSE_DATABASE|2|sent, deduplicated';
create table parted (p UInt64) engine = MergeTree order by tuple() partition by p % 8;
insert into parted select number % 1000 from numbers(100000) settings async_insert = 0;
select count() from (select id from events_dist) where id in (select p from parted) format Null settings force_creating_set_partitions_independently = 1, max_threads = 4;
set log_comment = '05529_$CLICKHOUSE_DATABASE|3|sent, if the filter is not';
select count() from (select id from events_dist) where id + rand() % 2 in (select number from numbers(100000)) format Null;

create table wide (id UInt64, g UInt64, w String) engine = MergeTree order by id settings index_granularity = 128, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
insert into wide select number, number, repeat('w', 100) from numbers(20000) settings async_insert = 0;
create table wide_dist as wide engine = Distributed(test_cluster_two_shards, currentDatabase(), wide);
set log_comment = '05529_$CLICKHOUSE_DATABASE|4|prewhere';
select sum(length(w)) from (select g, w from wide_dist) where g in (select id from some_dist) format Null
settings optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0, merge_tree_min_rows_for_concurrent_read = 128, merge_tree_min_bytes_for_concurrent_read = 1, read_in_order_use_buffering = 0;

set force_primary_key = 1, log_comment = '';
select count() from (select id from events_dist) where id in (select id from some_dist where id < 3);

set cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas', max_parallel_replicas = 2, use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;
set log_comment = '05529_$CLICKHOUSE_DATABASE|1|replicas';
select count() from (select distinct id from parts) where id in (select id from some_dist where id < 3)
settings enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, parallel_replicas_filter_pushdown = 1;
set log_comment = '05529_$CLICKHOUSE_DATABASE|2|replicas';
select count() from (select distinct id from parts) where id in (select id from some_dist where id < 3)
settings distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1;"

# An `EXPLAIN` sends nothing. Here, it prints the same as in other modes.
for mode in deny global
do
    $CLIENT --distributed_product_mode $mode --log_comment "05529_$CLICKHOUSE_DATABASE|5|sent with an explain, $mode" --network_compression_method none -q "
        set max_threads = 1; -- the number of threads depends on the load, and is printed
        explain pipeline distributed = 1 select count() from (select distinct id from events_dist) where id > 1;
        explain plan indexes = 1, distributed = 1 select count() from (select id from events_dist) where id in (select id from some_dist);
        explain pipeline distributed = 1 select count() from (select id from events_dist union all select id from some) where id in (select number from numbers(100000));" | md5sum
done | uniq | wc -l

$CLIENT -q "
system flush logs query_log;
select splitByChar('|', log_comment)[3], round(ProfileEvents['NetworkSendBytes'] / 1600000) from system.query_log
where $LOG and current_database = currentDatabase() and log_comment like '%|sent%' and is_initial_query and type = 'QueryFinish' and query_kind in ('Select', 'Explain')
order by log_comment;
select max(position(query, 'some_dist') > 0) ? 'text' : '-', max(position(query, '_data_') > 0) ? 'result' : '-' from system.query_log
where $LOG and log_comment like '%|replicas' and not is_initial_query and type = 'QueryStart' and match(query, 'parts|mergeTreeAnalyzeIndexes')
group by log_comment order by log_comment;
select 'prewhere', max(read_bytes) < 500000 from system.query_log
where $LOG and log_comment like '%|prewhere' and not is_initial_query and type = 'QueryFinish' and has(tables, currentDatabase() || '.wide');
select 'explain', uniqExact(query) from system.query_log
where $LOG and log_comment like '%explain%' and not is_initial_query and type = 'QueryStart' and query like 'EXPLAIN PIPELINE%DISTINCT%'"
