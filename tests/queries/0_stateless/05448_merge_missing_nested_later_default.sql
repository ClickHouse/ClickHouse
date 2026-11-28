SET flatten_nested = 1;

DROP TABLE IF EXISTS merge_missing_nested_later_default;
CREATE TABLE merge_missing_nested_later_default
(
    id UInt64,
    n Nested(i UInt64, s String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO merge_missing_nested_later_default VALUES (0, [1, 2], ['a', 'b']);
INSERT INTO merge_missing_nested_later_default VALUES (1, [3], ['c']);
ALTER TABLE merge_missing_nested_later_default ADD COLUMN n.a Array(String);
OPTIMIZE TABLE merge_missing_nested_later_default FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT arraySort(groupUniqArray(column))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'merge_missing_nested_later_default' AND active
    AND column IN ('id', 'n.i', 'n.s', 'n.a');

ALTER TABLE merge_missing_nested_later_default MODIFY COLUMN n.a Array(String) DEFAULT ['zzz'];
SELECT groupUniqArray(n.a) FROM merge_missing_nested_later_default;

DROP TABLE merge_missing_nested_later_default;