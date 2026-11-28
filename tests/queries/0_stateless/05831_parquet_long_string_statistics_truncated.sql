-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- A String bound longer than 4096 bytes is truncated instead of dropped, so the column index and the min/max
-- statistics of the column chunk are kept.

set engine_file_truncate_on_insert = 1;
set max_threads = 1, max_insert_threads = 1, max_block_size = 1000000;
set output_format_parquet_row_group_size = 1000, output_format_parquet_data_page_row_limit = 100, output_format_parquet_batch_size = 100;
set output_format_parquet_write_page_index = 1;
set input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1, input_format_parquet_use_offset_index = 1;

-- Row 1500 is the maximum of its page and row group, row 2000 the minimum of its page and row group.
insert into function file(currentDatabase() || '_a.parquet', Parquet)
select multiIf(number = 1500, repeat('z', 5000) || leftPad(toString(number), 8, '0'),
               number = 2000, leftPad(toString(number), 8, '0') || repeat('a', 5000),
               leftPad(toString(number), 8, '0')) as s
from numbers(4000);

select 'layout';
select num_row_groups, arrayMap(rg -> tupleElement(rg, 'num_rows'), row_groups) from file(currentDatabase() || '_a.parquet', ParquetMetadata);
select sum(length(s)) from file(currentDatabase() || '_a.parquet') settings log_comment = '05831_full_scan';

select 'chunk statistics';
select
    tupleElement(st[2], 'min') = '00001000',
    tupleElement(st[2], 'max') = repeat('z', 4095) || '{',
    tupleElement(st[3], 'min') = '00002000' || repeat('a', 4088),
    tupleElement(st[3], 'max') = '00002999'
from (select arrayMap(rg -> tupleElement(tupleElement(rg, 'columns')[1], 'statistics'), row_groups) as st
      from file(currentDatabase() || '_a.parquet', ParquetMetadata));
-- The truncated bounds are flagged inexact. In the Thrift compact encoding, `is_max_value_exact` and
-- `is_min_value_exact` follow `min_value` as one byte each: 0x11 is true, 0x12 is false.
select
    countSubstrings(raw, concat('{', unhex('1808'), '00001000', unhex('1211'))) > 0,
    countSubstrings(raw, concat(unhex('188020'), '00002000', repeat('a', 4088), unhex('1112'))) > 0
from file(currentDatabase() || '_a.parquet', RawBLOB, 'raw String');

select 'long values are not pruned';
select count() from file(currentDatabase() || '_a.parquet') where s = repeat('z', 5000) || '00001500';
select count() from file(currentDatabase() || '_a.parquet') where s > repeat('z', 4096);
select count() from file(currentDatabase() || '_a.parquet') where s = '00002000' || repeat('a', 5000);

select 'pruning';
select count() from file(currentDatabase() || '_a.parquet') where s between '00001700' and '00001709'
settings log_comment = '05831_page_pruning', input_format_parquet_dictionary_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0;
select count() from file(currentDatabase() || '_a.parquet') where s between '00000040' and '00000049'
settings log_comment = '05831_row_group_pruning', input_format_parquet_dictionary_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0;

system flush logs query_log;
select log_comment, ProfileEvents['ParquetReadPages'], ProfileEvents['ParquetPrunedPages'], ProfileEvents['ParquetPrunedRowGroups']
from system.query_log
where current_database = currentDatabase() and type = 'QueryFinish'
    and log_comment in ('05831_full_scan', '05831_page_pruning', '05831_row_group_pruning')
order by event_time_microseconds;

select 'utf-8';
-- The cut at 4096 bytes falls inside a 3-byte character, so the bounds are 4094 bytes long.
insert into function file(currentDatabase() || '_b.parquet', Parquet)
select multiIf(number = 0, '00000000' || repeat('€', 2000),
               number = 99, '00000099' || repeat('€', 2000),
               leftPad(toString(number), 8, '0')) as s
from numbers(100);
select isValidUTF8(mn), isValidUTF8(mx), mn = '00000000' || repeat('€', 1362), mx = '00000099' || repeat('€', 1361) || '₭'
from (select tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') as st, tupleElement(st, 'min') as mn, tupleElement(st, 'max') as mx
      from file(currentDatabase() || '_b.parquet', ParquetMetadata));
select count() from file(currentDatabase() || '_b.parquet') where s = '00000000' || repeat('€', 2000);
select count() from file(currentDatabase() || '_b.parquet') where s = '00000099' || repeat('€', 2000);

select 'no upper bound';
-- No string shorter than 4096 bytes is greater than one starting with 4096 0xFF bytes: no statistics, as before.
insert into function file(currentDatabase() || '_c.parquet', Parquet) select arrayJoin(['a', repeat(unhex('FF'), 5000)]) as s;
select tupleElement(st, 'min'), tupleElement(st, 'max')
from (select tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') as st from file(currentDatabase() || '_c.parquet', ParquetMetadata));
select count() from file(currentDatabase() || '_c.parquet') where s = repeat(unhex('FF'), 5000);
