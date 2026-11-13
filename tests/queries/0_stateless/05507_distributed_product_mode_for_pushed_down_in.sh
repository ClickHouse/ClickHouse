#!/usr/bin/env bash
# Tags: long, distributed, no-parallel, no-fasttest
# long: many queries; near the time limit of the flaky check under the thread fuzzer.
# no-fasttest: a failpoint.
# no-parallel: the failpoint, and uses the shared `shard_0` / `shard_1` databases of `test_cluster_two_shards_different_databases`.

# An `IN` written over a subquery, a view or a CTE that reads a `Distributed` table is pushed down to the shards
# (`allow_push_predicate_ast_for_distributed_subqueries`). With `distributed_product_mode = 'global'` or
# `prefer_global_in_and_join` the shards get the result of the right side rather than its text, like with `GLOBAL IN`. Every query
# below has to return the same number with the push-down off and on. A shard that runs the text of a right side that reads a
# `Distributed` table reads it in its turn, which `max_distributed_depth = 1` does not let it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The two "shards" are two databases of this server, with a local table of the same name as each `Distributed` table.
# Events 1..10 live on shard 0, 11..20 on shard 1. The lookup table is "crossed": each shard stores the ids whose events
# live on the other one. Ten ids have an event, but no shard finds any of them in its own lookup table.
$CLICKHOUSE_CLIENT -q "
create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists shard_0.events_05507;
drop table if exists shard_1.events_05507;
drop table if exists shard_0.lookup_05507;
drop table if exists shard_1.lookup_05507;

create table shard_0.events_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = MergeTree order by id;
create table shard_1.events_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = MergeTree order by id;
create table shard_0.lookup_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = MergeTree order by id;
create table shard_1.lookup_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = MergeTree order by id;

insert into shard_0.events_05507 select number, number % 4, if(number % 5 = 0, null, number) from numbers(1, 10);
insert into shard_1.events_05507 select number, number % 4, if(number % 5 = 0, null, number) from numbers(11, 10);
insert into shard_0.lookup_05507 select number, number % 4, if(number % 5 = 0, null, number) from numbers(11, 5);
insert into shard_1.lookup_05507 select number, number % 4, if(number % 5 = 0, null, number) from numbers(1, 5);

create table events_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = Distributed(test_cluster_two_shards_different_databases, '', events_05507);
create table lookup_05507 (id UInt64, g UInt64, n Nullable(Int64)) engine = Distributed(test_cluster_two_shards_different_databases, '', lookup_05507);
create table lookup_id (id UInt64) engine = Distributed(test_cluster_two_shards_different_databases, '', lookup_05507);
create table lookup_empty (id UInt64) engine = Distributed(test_cluster_two_shards, currentDatabase(), empty_local);
create table empty_local (id UInt64) engine = MergeTree order by id;
create table parts (id UInt64) engine = MergeTree order by id partition by id % 4 as select number from numbers(8);
create table some (id UInt64) engine = MergeTree order by id;
insert into some values (1), (2), (12), (100);

create view events_policy as select * from events_05507;
create row policy policy_05507 on events_policy using id in (select id from lookup_05507) to all;
create view events_policy_global as select * from events_05507;
create row policy policy_05507 on events_policy_global using id global in (select id from lookup_05507) to all;
create view events_view as select * from events_05507;
create view events_definer definer = current_user sql security definer as select count() as c from (select id from events_05507) where id in (select id from lookup_05507);
create view events_none sql security none as select count() as c from (select id from events_05507) where id in (select id from lookup_05507);
create table lookup_merge as lookup_05507 engine = Merge(currentDatabase(), '^lookup_05507\$');
create table lookup_buffer (id UInt64) engine = Buffer(currentDatabase(), lookup_05507, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
create view lookup_view as select id, g from lookup_05507 where id != 3;
create table events_merge as events_05507 engine = Merge(currentDatabase(), '^events_05507\$');
"

# name | query
QUERIES=$(cat <<'EOF'
subquery|select count() from (select id from events_05507) where id in (select id from lookup_05507)
view|select count() from events_view where id in (select id from lookup_05507)
cte|with e as (select id from events_05507) select count() from e where id in (select id from lookup_05507)
right side is a cte|with l as (select id from lookup_05507) select count() from events_view where id in l
right side reads a cte|with l as (select id from lookup_05507) select count() from events_view where id in (select id from l)
right side is a view|select count() from events_view where id in (select id from lookup_view)
right side is a recursive cte|with recursive r as (select id from lookup_05507 union all select id + 1000 as id from r where id < 0) select count() from events_view where id in (select id from r)
right side is cached|select count() from events_view where id in (select id from lookup_05507 settings use_query_cache = 1)
right side was cached|select count() from events_view where id in (select id from lookup_05507 settings use_query_cache = 1)
right side has another mode|select count() from events_view where id in (select id from lookup_05507 settings distributed_product_mode = 'allow')
right side reads a merge table, by a primary key|select count() from events_view where id in (select id from some where id in (select id from lookup_merge))
right side is a merge()|select count() from events_view where id in (select id from merge(currentDatabase(), '^lookup_05507$'))
right side is a buffer|select count() from events_view where id in (select id from lookup_buffer)
right side is a view, not in table|select count() from events_view where (id, g) not in lookup_view
in table|select count() from events_view where id in lookup_id
not in|select count() from events_view where id not in (select id from lookup_05507 where id < 14)
not in table|select count() from events_view where id not in lookup_id
tuple|select count() from events_view where (id, g) in (select id, g from lookup_05507)
tuple not in|select count() from events_view where (id, g) not in (select id, g + 1 from lookup_05507)
tuple of one column twice|select count() from events_view where (id, id) in (select id, id from lookup_05507)
expression on the left|select count() from events_view where id + 1 in (select id from lookup_05507)
nullable|select count() from events_view where n in (select n from lookup_05507)
nullable not in|select count() from events_view where n not in (select n from lookup_05507)
nullable, transform_null_in|select count() from events_view where n in (select n from lookup_05507) settings transform_null_in = 1
empty right side|select count() from events_view where id in (select id from lookup_empty)
not in empty right side|select count() from events_view where id not in (select id from lookup_empty)
set on disk|select count() from events_view where id in (select id from lookup_05507) settings max_bytes_before_external_set = 1
set on disk, not in|select count() from events_view where id not in (select id from lookup_05507 union all select id from lookup_05507) settings max_bytes_before_external_set = 1
right side is limited|select count() from events_view where id in (select id from lookup_05507 order by id limit 3)
right side is aggregated|select count() from events_view where id in (select max(id) from lookup_05507 group by g)
right side is aliased|select count() from events_view where id in (select l.id as x from lookup_05507 as l)
right side is a union all|select count() from events_view where id in (select id from lookup_05507 union all select id from some)
right side is a union distinct|select count() from events_view where id in (select id from some union distinct select id from lookup_05507)
right side joins|select count() from events_view where id in (select l.id from some s join (select id from lookup_05507) l on s.id = l.id)
right side is local|select count() from events_view where id in (select id from some)
right side is local, nested in|select count() from events_view where id in (select number from numbers(30) where number in (select id from lookup_05507))
right side is local, nested in by a primary key|select count() from events_view where id in (select id from some where id in (select id from lookup_05507))
right side has a global in|select count() from events_view where id in (select number from numbers(1, 20) where number global in (select id from lookup_05507))
right side has a global join|select count() from events_view where id in (select s.id from some s global join (select id from lookup_05507) l on s.id = l.id)
right side has its own mode, global|select count() from (select id from events_05507) where id in (select id from lookup_05507 settings distributed_product_mode = 'global')
and|select count() from events_view where id > 2 and id in (select id from lookup_05507)
and, three|select count() from events_view where id > 2 and id in (select id from lookup_05507) and g != 1
or|select count() from events_view where g = 0 or id in (select id from lookup_05507)
two, or|select count() from events_view where id not in (select id from lookup_05507) or id in (select id from some)
two, equal|select count() from events_view where (id in (select id from lookup_05507)) = (g in (select id from lookup_merge))
or with an and inside|select count() from events_view where g = 0 or (id > 2 and id in (select id from lookup_05507))
not|select count() from events_view where not (id in (select id from lookup_05507))
not and|select count() from events_view where not (id > 2 and id in (select id from lookup_05507))
not or|select count() from events_view where not (id > 12 or id in (select id from lookup_05507))
if|select count() from events_view where if(g = 0, id in (select id from lookup_05507), id > 5)
xor|select count() from events_view where xor(g = 0, id in (select id from lookup_05507))
comparison|select count() from events_view where (id in (select id from lookup_05507)) = 0
same right side twice|select count() from events_view where id in (select id from lookup_05507) and (id + 1) in (select id from lookup_05507)
union all|select count() from (select id from events_05507 union all select id from events_05507) where id in (select id from lookup_05507)
union distinct|select count() from (select id from events_05507 union distinct select id from events_view) where id in (select id from lookup_05507)
merge over distributed|select count() from (select id from events_merge) where id in (select id from lookup_05507)
prewhere|select count() from (select id from events_05507 prewhere g < 3) where id in (select id from lookup_05507)
limit below|select count() from (select id from events_05507 order by id limit 13) where id in (select id from lookup_05507)
limit by below|select count() from (select id, g from events_05507 order by id limit 2 by g) where id in (select id from lookup_05507)
distinct below|select count() from (select distinct id from events_05507) where id in (select id from lookup_05507)
group by below|select count() from (select id, count() from events_05507 group by id) where id in (select id from lookup_05507)
group by expression below|select count() from (select id + 1 as x, count() from events_05507 group by x) where x in (select id from lookup_05507)
window below|select count() from (select id, g, row_number() over (partition by g order by id) as r from events_05507) where g in (select id from lookup_05507)
join below|select count() from (select e.id from (select id from events_05507) e join some s on e.id = s.id) where id in (select id from lookup_05507)
join above|select count() from (select id from events_05507) e join some s on e.id = s.id where e.id in (select id from lookup_05507)
row policy|select count() from merge(currentDatabase(), '^events_policy$')
row policy with a global in|select count() from merge(currentDatabase(), '^events_policy_global$')
view with a definer|select c from events_definer
view without sql security|select c from events_none
group by above|select sum(c) from (select g, count() as c from events_view where id in (select id from lookup_05507) group by g having c > 1)
limit above|select groupArray(id) from (select id from events_view where id in (select id from lookup_05507) order by id limit 7)
order by above|select groupArray(id) from (select id from events_view where id in (select id from lookup_05507) order by id desc)
distinct above|select count() from (select distinct g from events_view where id in (select id from lookup_05507))
window above|select max(s) from (select sum(id) over (order by id) as s from events_view where id in (select id from lookup_05507))
without merging|select sum(c) from (select count() as c from (select id from events_05507) where id in (select id from lookup_05507)) settings distributed_group_by_no_merge = 1
EOF
)

function queries()
{
    while IFS='|' read -r name query
    do
        echo "select '$name', * from ($query);"
    done <<< "$QUERIES"
}

expected=${CLICKHOUSE_TMP}/05507_${CLICKHOUSE_DATABASE}_expected
actual=${CLICKHOUSE_TMP}/05507_${CLICKHOUSE_DATABASE}_actual

# `prefer_localhost_replica = 0`: the shards are this very server, make the initiator talk to them as to remote ones.
# Parallel replicas and a second hop would add to the depth by themselves.
function run()
{
    (echo "set enable_analyzer = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0; set $1;"; queries) | $CLICKHOUSE_CLIENT --ignore-error 2>&1
}

run "distributed_product_mode = 'deny', allow_push_predicate_ast_for_distributed_subqueries = 0" | tee "$expected"

for settings in \
    "distributed_product_mode = 'global'" \
    "distributed_product_mode = 'deny', prefer_global_in_and_join = 1"
do
    run "$settings, max_distributed_depth = 1" > "$actual"
    echo "$settings: $(diff "$expected" "$actual" > "$actual.diff" && echo 'same' || cat "$actual.diff")"
done

# The mode is the one where the pushed down `IN` ends up.
echo '-- settings of a scope'
QUERIES=$(cat <<'EOF'
outer local, inner global|select count() from (select id from events_05507 settings distributed_product_mode = 'global') where id in (select id from lookup_05507) settings distributed_product_mode = 'local'
outer deny, inner prefer global|select count() from (select id from events_05507 settings prefer_global_in_and_join = 1) where id in (select id from lookup_05507) settings distributed_product_mode = 'deny'
EOF
)
run "distributed_product_mode = 'deny', max_distributed_depth = 1"

# What is still sent as text.
echo '-- as before'
QUERIES=$(cat <<'EOF'
same right side, global and local|select count() from events_view where id global in (select id from lookup_05507) and (id + 0) in (select id from lookup_05507)
same right side, local and global|select count() from events_view where (id + 0) in (select id from lookup_05507) and id global in (select id from lookup_05507)
same right side, global or local|select count() from events_view where id global in (select id from lookup_05507) or (id + 1) in (select id from lookup_05507)
row policy with a global in, and an in|select count() from merge(currentDatabase(), '^events_policy_global$') where id in (select id from lookup_05507)
set built for a virtual column|select count() from (select _partition_id as p from events_05507 union all select _partition_id from parts) where p in (select toString(g) from lookup_05507)
set built for a virtual column, not in|select count() from (select _partition_id as p from events_05507 union all select _partition_id from parts) where p not in (select toString(g) from lookup_05507)
EOF
)
run "distributed_product_mode = 'global'"

# The same `IN` written over the `Distributed` table directly is none of this: there the mode is applied.
echo '-- directly over the Distributed table'
$CLICKHOUSE_CLIENT --enable_analyzer 1 --prefer_localhost_replica 0 -q "
select 'global', count() from events_05507 where id in (select id from lookup_05507) settings distributed_product_mode = 'global';
select 'local', count() from events_05507 where id in (select id from lookup_05507) settings distributed_product_mode = 'local';
select 'deny', count() from events_05507 where id in (select id from lookup_05507) settings distributed_product_mode = 'deny'; -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
"

echo '-- lazily'
query_id="05507_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --enable_analyzer 1 --prefer_localhost_replica 0 --enable_parallel_replicas 0 --serialize_query_plan 0 --distributed_product_mode global --network_compression_method none -q "
system enable failpoint use_delayed_remote_source;
select count() from events_view where id in (select id from lookup_05507) settings max_distributed_depth = 1;"
$CLICKHOUSE_CLIENT --enable_analyzer 1 --prefer_localhost_replica 0 --enable_parallel_replicas 0 --serialize_query_plan 0 --distributed_product_mode global --network_compression_method none --query_id "$query_id" -q "
explain pipeline distributed = 1 select count() from (select id from events_05507 union all select id from some) where id in (select number from numbers(100000))" > /dev/null
$CLICKHOUSE_CLIENT -q "
system disable failpoint use_delayed_remote_source;
system flush logs query_log;
select 'sent with an explain', round(ProfileEvents['NetworkSendBytes'] / 1600000) from system.query_log
where event_date >= yesterday() and event_time >= now() - 600 and current_database = currentDatabase() and query_id = '$query_id' and type = 'QueryFinish'"

# The result is limited as with `GLOBAL IN`.
$CLICKHOUSE_CLIENT --enable_analyzer 1 --prefer_localhost_replica 0 --enable_parallel_replicas 0 --serialize_query_plan 0 -q "
select count() from events_view where id in (select id from lookup_05507) settings distributed_product_mode = 'global', max_rows_to_transfer = 2; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
select count() from events_view where id + rand() % 1 in (select id from lookup_05507) settings distributed_product_mode = 'global', max_rows_to_transfer = 2;
"

rm -f "$expected" "$actual" "$actual.diff"
$CLICKHOUSE_CLIENT -q "
drop row policy policy_05507 on events_policy;
drop row policy policy_05507 on events_policy_global;
drop table shard_0.events_05507;
drop table shard_1.events_05507;
drop table shard_0.lookup_05507;
drop table shard_1.lookup_05507;
"
