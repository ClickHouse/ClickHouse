-- Tags: no-darwin
-- Case-insensitive filesystems hash every stream name, including the literal hash below, so the names do not collide there.

DROP TABLE IF EXISTS t_dynamic_stream_collision;

-- The dynamic `JSON` path is unknown at `CREATE` time. Its `Int64` stream hashes to the other column's literal name.
SELECT lower(hex(reverse(sipHash128(concat('j.', repeat('a', 160), '.Int64'))))) = '16799e7a4f30f4f83e744fcf2214fd41';

CREATE TABLE t_dynamic_stream_collision
(
    j JSON,
    `16799e7a4f30f4f83e744fcf2214fd41` Int64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    replace_long_file_name_to_hash = 1, max_file_name_length = 127,
    ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_dynamic_stream_collision
SELECT CAST(concat('{"', repeat('a', 160), '":11}'), 'JSON'), toInt64(99); -- { serverError INCORRECT_FILE_NAME }

-- Reject the write before committing a part with shared data and marks files.
SELECT count() FROM t_dynamic_stream_collision;

INSERT INTO t_dynamic_stream_collision VALUES ('{"ok":22}', 99);
SELECT toJSONString(j), `16799e7a4f30f4f83e744fcf2214fd41` FROM t_dynamic_stream_collision;

DROP TABLE t_dynamic_stream_collision;
