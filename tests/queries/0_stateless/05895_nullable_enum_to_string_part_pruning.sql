DROP TABLE IF EXISTS nullable_enum_to_string;

SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;

-- The numeric codes and the string labels sort in opposite orders.
CREATE TABLE nullable_enum_to_string (c Nullable(Enum8('b' = 1, 'B' = 2)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';

INSERT INTO nullable_enum_to_string VALUES ('b'), ('B');

SELECT count() FROM nullable_enum_to_string WHERE toString(c) = 'b';
SELECT count() FROM nullable_enum_to_string WHERE toString(c) >= '_a';

DROP TABLE nullable_enum_to_string;
