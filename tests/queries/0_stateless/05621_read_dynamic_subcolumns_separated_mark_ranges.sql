-- Random settings limits: compute_exact_num_defaults_for_sparse_columns=(1, 1); ratio_of_defaults_for_sparse_serialization=(0.5, 0.5)
-- A block assembled from separated mark ranges appends to the result columns the reader
-- created after the first range: those must keep passing the serialization check. The
-- `d.JSON` and `v.JSON` subcolumns read into a Nullable(JSON) column through
-- SerializationDynamicElement/SerializationVariantElement, which peel the extraction's
-- Nullable off before the nested serialization appends to the column.

set allow_experimental_dynamic_type = 1;
set allow_experimental_variant_type = 1;

drop table if exists dynamic_wide;
drop table if exists dynamic_compact;
drop table if exists variant_wide;
drop table if exists sparse_tuple_wide;

create table dynamic_wide (id UInt64, d Dynamic) engine=MergeTree order by id
    settings index_granularity=100, min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into dynamic_wide select number, if(number % 4 = 0, NULL, cast('{"a": ' || toString(number) || '}', 'JSON')) from numbers(10000);

create table dynamic_compact (id UInt64, d Dynamic) engine=MergeTree order by id
    settings index_granularity=100, min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;
insert into dynamic_compact select number, if(number % 4 = 0, NULL, cast('{"a": ' || toString(number) || '}', 'JSON')) from numbers(10000);

create table variant_wide (id UInt64, v Variant(JSON, String)) engine=MergeTree order by id
    settings index_granularity=100, min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into variant_wide select number, if(number % 4 = 0, NULL, cast('{"a": ' || toString(number) || '}', 'JSON')) from numbers(10000);

-- A tuple element can be stored sparsely; reading it goes through the element serialization,
-- and the serialization-created tuple columns must stay accepted across ranges too. The plain
-- column makes the sparse layout of the part observable through system.parts_columns.
create table sparse_tuple_wide (id UInt64, t Tuple(s String, x UInt64), plain String) engine=MergeTree order by id
    settings index_granularity=100, min_rows_for_wide_part=1, min_bytes_for_wide_part=1, ratio_of_defaults_for_sparse_serialization=0.5;
insert into sparse_tuple_wide select number, tuple(if(number % 20 = 0, 'v' || toString(number), ''), number), if(number % 20 = 0, 'v' || toString(number), '') from numbers(6000);

-- The part layout is fixed by the table settings; the number of parts is not
-- (the randomized test settings may split the inserts).
select distinct part_type from system.parts
where database = currentDatabase() and active
    and table in ('dynamic_wide', 'dynamic_compact', 'variant_wide', 'sparse_tuple_wide')
order by part_type;

select count(), countIf(d.JSON is not NULL) from dynamic_wide
where id < 3000 or (id >= 5000 and id < 8000) or id >= 9500;
select count(), countIf(d.JSON is not NULL) from dynamic_compact
where id < 3000 or (id >= 5000 and id < 8000) or id >= 9500;
select count(), countIf(v.JSON is not NULL) from variant_wide
where id < 3000 or (id >= 5000 and id < 8000) or id >= 9500;
select count(), countIf(t.s != '') from sparse_tuple_wide
where id < 2000 or (id >= 3000 and id < 4000) or id >= 5500;

-- Sanity: the part does use the sparse layout.
select distinct column, serialization_kind from system.parts_columns
where database = currentDatabase() and table = 'sparse_tuple_wide' order by column;

drop table dynamic_wide;
drop table dynamic_compact;
drop table variant_wide;
drop table sparse_tuple_wide;
