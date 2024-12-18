-- Tags: global

set allow_prefetched_read_pool_for_remote_filesystem=0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0, max_threads=2, max_block_size=65387;
set max_rows_to_read = '100M';

SELECT sum(UserID GLOBAL IN (SELECT UserID FROM remote('127.0.0.{1,2}', test.hits))) FROM remote('127.0.0.{1,2}', test.hits);
SELECT sum(UserID GLOBAL IN (SELECT UserID FROM test.hits)) FROM remote('127.0.0.{1,2}', test.hits);
