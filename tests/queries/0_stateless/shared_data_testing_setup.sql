set use_variant_as_common_type=1;

create or replace table test_compact_10_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_compact_10_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_compact_10_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;
create or replace table test_compact_10_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;

create or replace table test_wide_10_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_wide_10_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;
create or replace table test_wide_10_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;


create or replace table test_compact_100_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_compact_100_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_compact_100_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_100_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;
create or replace table test_compact_100_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;

create or replace table test_wide_100_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_wide_100_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;
create or replace table test_wide_100_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;


create or replace table test_compact_1000_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_compact_1000_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;
create or replace table test_compact_1000_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;

create or replace table test_wide_1000_paths_json_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_wide_1000_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;


create or replace table test_compact_10000_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_compact_10000_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;
create or replace table test_compact_10000_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', old_parts_lifetime=1;

create or replace table test_wide_10000_paths_json_shared_data_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2', old_parts_lifetime=1;
create or replace table test_wide_10000_paths_json_shared_data_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_string (json String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_map (json Map(String, String)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, old_parts_lifetime=1;



WITH
    10 AS num_paths,
    1000000 AS number_of_rows
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);



10 paths
------------

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_compact_10_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_compact_10_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_compact_10_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_compact_10_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_compact_10_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

------------


WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_wide_10_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_wide_10_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_wide_10_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_wide_10_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10 AS num_paths,
    5000000 AS number_of_rows
INSERT INTO test_wide_10_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

-----------
100 paths
-----------
WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_compact_100_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_compact_100_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_compact_100_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_compact_100_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_compact_100_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

------------


WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_wide_100_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_wide_100_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_wide_100_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_wide_100_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    2000000 AS number_of_rows
INSERT INTO test_wide_100_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

------------
1000 paths
------------

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_compact_1000_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_compact_1000_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_compact_1000_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_compact_1000_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_compact_1000_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

------------


WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_wide_1000_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_wide_1000_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_wide_1000_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_wide_1000_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    1000 AS num_paths,
    1000000 AS number_of_rows
INSERT INTO test_wide_1000_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

--------------
10000 paths
--------------

WITH
    10000 AS num_paths,
    200000 AS number_of_rows
INSERT INTO test_compact_10000_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    100 AS num_paths,
    200000 AS number_of_rows
INSERT INTO test_compact_10000_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    200000 AS number_of_rows
INSERT INTO test_compact_10000_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    200000 AS number_of_rows
INSERT INTO test_compact_10000_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    200000 AS number_of_rows
INSERT INTO test_compact_10000_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);

------------


WITH
    10000 AS num_paths,
    500000 AS number_of_rows
INSERT INTO test_wide_10000_paths_json_no_shared_data
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    500000 AS number_of_rows
INSERT INTO test_wide_10000_paths_json_shared_data_old
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    500000 AS number_of_rows
INSERT INTO test_wide_10000_paths_json_shared_data_new
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    500000 AS number_of_rows
INSERT INTO test_wide_10000_paths_map
SELECT arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64) FROM numbers(number_of_rows);

WITH
    10000 AS num_paths,
    500000 AS number_of_rows
INSERT INTO test_wide_10000_paths_string
SELECT toJSONString(arrayMap(x -> tuple('key' || x, x + number), range(num_paths))::Map(String, Int64)) FROM numbers(number_of_rows);










SELECT CAST(arrayMap(x -> (if(x = num_paths_in_shared_data, 'arr', concat('a', x)), multiIf(x = 0, number, x = num_paths_in_shared_data, arrayMap(y -> CAST(arrayMap(z -> (concat('b', z), z), range(nested_array_json_size)), 'Map(String, Int64)'), range(number % 5)), (x % 5) = 1, x + number, (x % 5) = 2, randomStringUTF8(rand64() % 10), (x % 5) = 3, range(rand64() % 5), (x % 5) = 4, now(), (x + number) / 17)), range(num_paths_in_shared_data + 1)), 'Map(String, Dynamic)') AS json
FROM numbers(number_of_rows)
INTO OUTFILE 'data_100_paths.json' TRUNCATE
FORMAT JSONEachRow
SETTINGS output_format_json_quote_64bit_integers = 0, use_variant_as_common_type = 1, max_block_size = 10000









create or replace table test_compact_10_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_compact_100_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;


create or replace table test_compact_10000_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_no_shared_data (json JSON(max_dynamic_paths=100000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;

insert into test_compact_10_paths_no_shared_data from infile 'data_10_paths.json.gz' format JSONEachRow;
insert into test_wide_10_paths_no_shared_data from infile 'data_10_paths.json.gz' format JSONEachRow;
insert into test_compact_100_paths_no_shared_data from infile 'data_100_paths.json.gz' format JSONEachRow;
insert into test_wide_100_paths_no_shared_data from infile 'data_100_paths.json.gz' format JSONEachRow;
insert into test_compact_1000_paths_no_shared_data from infile 'data_1000_paths.json.gz' format JSONEachRow;
insert into test_wide_1000_paths_no_shared_data from infile 'data_1000_paths.json.gz' format JSONEachRow;
insert into test_compact_10000_paths_no_shared_data from infile 'data_10000_paths.json.gz' format JSONEachRow;
insert into test_wide_10000_paths_no_shared_data from infile 'data_10000_paths.json.gz' format JSONEachRow;




create or replace table test_compact_10_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_10_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_10_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_wide_10_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_10_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_10_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

WITH
    10 AS num_paths_in_shared_data,
    10 AS nested_array_json_size,
    1000000 AS number_of_rows
SELECT CAST(arrayMap(x -> (if(x = num_paths_in_shared_data, 'arr', concat('a', x)), multiIf(x = 0, number, x = num_paths_in_shared_data, arrayMap(y -> CAST(arrayMap(z -> (concat('b', z), z), range(nested_array_json_size)), 'Map(String, Int64)'), range(number % 5)), (x % 5) = 1, x + number, (x % 5) = 2, randomStringUTF8(rand64() % 10), (x % 5) = 3, range(rand64() % 5), (x % 5) = 4, now(), (x + number) / 17)), range(num_paths_in_shared_data + 1)), 'Map(String, Dynamic)') AS json
FROM numbers(number_of_rows)
INTO OUTFILE 'data_100_paths.json' TRUNCATE
FORMAT JSONEachRow
SETTINGS output_format_json_quote_64bit_integers = 0, use_variant_as_common_type = 1, max_block_size = 10000

insert into test_compact_10_paths_1_bucket_old from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_8_bucket_old from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_32_bucket_old from infile 'data_10_paths.json' format JSONEachRow;

insert into test_wide_10_paths_1_bucket_old from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_8_bucket_old from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_32_bucket_old from infile 'data_10_paths.json' format JSONEachRow;


insert into test_compact_10_paths_1_bucket_new from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_8_bucket_new from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_32_bucket_new from infile 'data_10_paths.json' format JSONEachRow;

insert into test_wide_10_paths_1_bucket_new from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_8_bucket_new from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_32_bucket_new from infile 'data_10_paths.json' format JSONEachRow;


insert into test_compact_10_paths_1_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_8_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;
insert into test_compact_10_paths_32_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;

insert into test_wide_10_paths_1_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_8_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;
insert into test_wide_10_paths_32_bucket_new_with_substreams from infile 'data_10_paths.json' format JSONEachRow;



create or replace table test_compact_100_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_100_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_100_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_100_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_100_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_100_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_100_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_100_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_100_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_wide_100_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_100_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_100_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_100_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_100_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_100_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_100_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

WITH
    100 AS num_paths_in_shared_data,
    100 AS nested_array_json_size,
    1000000 AS number_of_rows
SELECT CAST(arrayMap(x -> (if(x = num_paths_in_shared_data, 'arr', concat('a', x)), multiIf(x = 0, number, x = num_paths_in_shared_data, arrayMap(y -> CAST(arrayMap(z -> (concat('b', z), z), range(nested_array_json_size)), 'Map(String, Int64)'), range(number % 5)), (x % 5) = 1, x + number, (x % 5) = 2, randomStringUTF8(rand64() % 10), (x % 5) = 3, range(rand64() % 5), (x % 5) = 4, now(), (x + number) / 17)), range(num_paths_in_shared_data + 1)), 'Map(String, Dynamic)') AS json
FROM numbers(number_of_rows)
INTO OUTFILE 'data_100_paths.json' TRUNCATE
FORMAT JSONEachRow
SETTINGS output_format_json_quote_64bit_integers = 0, use_variant_as_common_type = 1, max_block_size = 10000


insert into test_compact_100_paths_1_bucket_old from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_8_bucket_old from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_32_bucket_old from infile 'data_100_paths.json' format JSONEachRow;

insert into test_wide_100_paths_1_bucket_old from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_8_bucket_old from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_32_bucket_old from infile 'data_100_paths.json' format JSONEachRow;


insert into test_compact_100_paths_1_bucket_new from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_8_bucket_new from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_32_bucket_new from infile 'data_100_paths.json' format JSONEachRow;

insert into test_wide_100_paths_1_bucket_new from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_8_bucket_new from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_32_bucket_new from infile 'data_100_paths.json' format JSONEachRow;


insert into test_compact_100_paths_1_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_8_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;
insert into test_compact_100_paths_32_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;

insert into test_wide_100_paths_1_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_8_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;
insert into test_wide_100_paths_32_bucket_new_with_substreams from infile 'data_100_paths.json' format JSONEachRow;




create or replace table test_compact_1000_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_1000_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_1000_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_1000_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_wide_1000_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_1000_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_1000_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_1000_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

WITH
    1000 AS num_paths_in_shared_data,
    100 AS nested_array_json_size,
    500000 AS number_of_rows
SELECT CAST(arrayMap(x -> (if(x = num_paths_in_shared_data, 'arr', concat('a', x)), multiIf(x = 0, number, x = num_paths_in_shared_data, arrayMap(y -> CAST(arrayMap(z -> (concat('b', z), z), range(nested_array_json_size)), 'Map(String, Int64)'), range(number % 5)), (x % 5) = 1, x + number, (x % 5) = 2, randomStringUTF8(rand64() % 10), (x % 5) = 3, range(rand64() % 5), (x % 5) = 4, now(), (x + number) / 17)), range(num_paths_in_shared_data + 1)), 'Map(String, Dynamic)') AS json
FROM numbers(number_of_rows)
INTO OUTFILE 'data_1000_paths.json' TRUNCATE
FORMAT JSONEachRow
SETTINGS output_format_json_quote_64bit_integers = 0, use_variant_as_common_type = 1, max_block_size = 10000


insert into test_compact_1000_paths_1_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_8_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_32_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;

insert into test_wide_1000_paths_1_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_8_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_32_bucket_old from infile 'data_1000_paths.json' format JSONEachRow;


insert into test_compact_1000_paths_1_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_8_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_32_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;

insert into test_wide_1000_paths_1_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_8_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_32_bucket_new from infile 'data_1000_paths.json' format JSONEachRow;


insert into test_compact_1000_paths_1_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_8_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_compact_1000_paths_32_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;

insert into test_wide_1000_paths_1_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_8_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;
insert into test_wide_1000_paths_32_bucket_new_with_substreams from infile 'data_1000_paths.json' format JSONEachRow;



create or replace table test_compact_10000_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_10000_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_compact_10000_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table test_compact_10000_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table test_wide_10000_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_10000_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table test_wide_10000_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table test_wide_10000_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

WITH
    1000 AS num_paths_in_shared_data,
    100 AS nested_array_json_size,
    500000 AS number_of_rows
SELECT CAST(arrayMap(x -> (if(x = num_paths_in_shared_data, 'arr', concat('a', x)), multiIf(x = 0, number, x = num_paths_in_shared_data, arrayMap(y -> CAST(arrayMap(z -> (concat('b', z), z), range(nested_array_json_size)), 'Map(String, Int64)'), range(number % 5)), (x % 5) = 1, x + number, (x % 5) = 2, randomStringUTF8(rand64() % 10), (x % 5) = 3, range(rand64() % 5), (x % 5) = 4, now(), (x + number) / 17)), range(num_paths_in_shared_data + 1)), 'Map(String, Dynamic)') AS json
FROM numbers(number_of_rows)
INTO OUTFILE 'data_10000_paths.json' TRUNCATE
FORMAT JSONEachRow
SETTINGS output_format_json_quote_64bit_integers = 0, use_variant_as_common_type = 1, max_block_size = 10000


insert into test_compact_10000_paths_1_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_8_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_32_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;

insert into test_wide_10000_paths_1_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_8_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_32_bucket_old from infile 'data_10000_paths.json' format JSONEachRow;


insert into test_compact_10000_paths_1_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_8_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_32_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;

insert into test_wide_10000_paths_1_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_8_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_32_bucket_new from infile 'data_10000_paths.json' format JSONEachRow;


insert into test_compact_10000_paths_1_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_8_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_compact_10000_paths_32_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;

insert into test_wide_10000_paths_1_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_8_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;
insert into test_wide_10000_paths_32_bucket_new_with_substreams from infile 'data_10000_paths.json' format JSONEachRow;

create or replace table test_compact_1000000_unique_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

create or replace table test_compact_1000000_unique_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

create or replace table test_compact_1000000_unique_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_compact_1000000_unique_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1000000000, min_rows_for_wide_part=1000000000, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

create or replace table test_wide_1000000_unique_paths_1_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_8_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_32_bucket_old (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v2_with_buckets', object_serialization_version_for_zero_level_parts='v2_with_buckets', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

create or replace table test_wide_1000000_unique_paths_1_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_8_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_32_bucket_new (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v3', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

create or replace table test_wide_1000000_unique_paths_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=1, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=8, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;
create or replace table test_wide_1000000_unique_paths_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, object_serialization_version='v4', object_serialization_version_for_zero_level_parts='v3', object_shared_data_buckets=32, old_parts_lifetime=1, write_marks_for_substreams_in_compact_parts=1;

insert into test_compact_1000000_unique_paths_1_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_8_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_32_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);

insert into test_compact_1000000_unique_paths_1_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_8_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_32_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);

insert into test_compact_1000000_unique_paths_1_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_8_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_compact_1000000_unique_paths_32_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);

insert into test_wide_1000000_unique_paths_1_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_8_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_32_bucket_old select map('a' || number, number::UInt32) from numbers(1000000);

insert into test_wide_1000000_unique_paths_1_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_8_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_32_bucket_new select map('a' || number, number::UInt32) from numbers(1000000);

insert into test_wide_1000000_unique_paths_1_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_8_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);
insert into test_wide_1000000_unique_paths_32_bucket_new_with_substreams select map('a' || number, number::UInt32) from numbers(1000000);

compression 'gzip'

create or replace table gharchive_compact_no_shared_data (json JSON(max_dynamic_paths=10000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;
create or replace table gharchive_wide_no_shared_data (json JSON(max_dynamic_paths=10000)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, old_parts_lifetime=1;

create or replace table gharchive_compact_1_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table gharchive_compact_8_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table gharchive_compact_32_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table gharchive_compact_1_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table gharchive_compact_8_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table gharchive_compact_32_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table gharchive_compact_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=1, old_parts_lifetime=1;
create or replace table gharchive_compact_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=8, old_parts_lifetime=1;
create or replace table gharchive_compact_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_compact_part=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets_for_wide_part=32, old_parts_lifetime=1;



create or replace table gharchive_compact_4_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_compact_16_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=16, old_parts_lifetime=1;

create or replace table gharchive_compact_4_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_compact_16_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=16, old_parts_lifetime=1;

create or replace table gharchive_compact_4_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_compact_16_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=16, old_parts_lifetime=1;

create or replace table gharchive_wide_4_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_wide_16_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=16, old_parts_lifetime=1;

create or replace table gharchive_wide_4_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_wide_16_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=16, old_parts_lifetime=1;

create or replace table gharchive_wide_4_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=4, old_parts_lifetime=1;
create or replace table gharchive_wide_16_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=16, old_parts_lifetime=1;



create or replace table gharchive_compact_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=1, old_parts_lifetime=1;
create or replace table gharchive_compact_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=8, old_parts_lifetime=1;
create or replace table gharchive_compact_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G', write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_old (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v2_with_buckets', object_shared_data_buckets=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_new (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v3', object_shared_data_buckets=32, old_parts_lifetime=1;

create or replace table gharchive_wide_1_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=1, old_parts_lifetime=1;
create or replace table gharchive_wide_8_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=8, old_parts_lifetime=1;
create or replace table gharchive_wide_32_bucket_new_with_substreams (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, write_marks_for_substreams_in_compact_parts=1, object_serialization_version='v4', object_shared_data_buckets=32, old_parts_lifetime=1;






$client -m < fill_10_paths_wide.sql &
$client -m < fill_10_paths_compact.sql &
$client -m < fill_100_paths_compact.sql &
$client -m < fill_100_paths_wide.sql &
$client -m < fill_1000_paths_compact_string_and_map.sql &
$client -m < fill_1000_paths_wide_string_and_map.sql &
$client -m < fill_1000_paths_compact_json_old_and_no_shared_data.sql &
$client -m < fill_1000_paths_wide_json_old_and_no_shared_data.sql &
$client -m < fill_1000_paths_compact_json_new_shared_data.sql &
$client -m < fill_1000_paths_wide_json_new_shared_data.sql &

$client -m < fill_10000_paths_compact_string_and_map.sql &
$client -m < fill_10000_paths_wide_string_and_map.sql &
$client -m < fill_10000_paths_compact_json_old_and_no_shared_data.sql &
$client -m < fill_10000_paths_wide_json_old_and_no_shared_data.sql &
$client -m < fill_10000_paths_compact_json_new_shared_data.sql &
$client -m < fill_10000_paths_wide_json_new_shared_data.sql &