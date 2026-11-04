-- `jsonbf_v1` is experimental in every new table definition, including indexes copied by `CREATE TABLE ... AS`.
-- Attaching a table that already has the index keeps working without the setting.
SET allow_experimental_json_bloom_filter_index = 1;
CREATE TABLE json_bf_create_as_src (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1) ENGINE = MergeTree ORDER BY id;

SET allow_experimental_json_bloom_filter_index = 0;
CREATE TABLE json_bf_create_as_copy AS json_bf_create_as_src; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_json_bloom_filter_index = 1;
CREATE TABLE json_bf_create_as_copy AS json_bf_create_as_src;

SET allow_experimental_json_bloom_filter_index = 0;
DETACH TABLE json_bf_create_as_copy;
ATTACH TABLE json_bf_create_as_copy;
SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'json_bf_create_as_copy';

DROP TABLE json_bf_create_as_copy;
DROP TABLE json_bf_create_as_src;
