-- A missing non-`Nullable` typed path reads as its default, which `jsonbf_v1` does not index, and a cast to a
-- `Nullable` type keeps that default. Path presence must not be used to skip granules for such predicates.
SET allow_experimental_json_bloom_filter_index = 1;

CREATE TABLE json_bf_typed_path_presence_casts (id UInt64, j JSON(x UInt8), INDEX bf j TYPE jsonbf_v1 GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_bf_typed_path_presence_casts VALUES (1, '{"y": 1}'), (2, '{"x": 5}');

SELECT arraySort(groupArray(id)) FROM json_bf_typed_path_presence_casts WHERE CAST(j.x AS Nullable(UInt8)) = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_typed_path_presence_casts WHERE CAST(j.x AS Nullable(UInt8)) IN (0);
SELECT arraySort(groupArray(id)) FROM json_bf_typed_path_presence_casts WHERE isNotNull(CAST(j.x AS Nullable(UInt8)));

DROP TABLE json_bf_typed_path_presence_casts;
