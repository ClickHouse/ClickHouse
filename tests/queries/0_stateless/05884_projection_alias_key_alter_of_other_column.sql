-- an `ALTER` of a column checks the keys of each projection; a key can be an alias of the projection query

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_projection_alias_key_alter;

CREATE TABLE t_projection_alias_key_alter
(
    a UInt8,
    j JSON,
    t Tuple(x UInt8),
    PROJECTION p_select (SELECT a % 2 AS k, count() GROUP BY k),
    PROJECTION p_with (WITH a % 3 AS k SELECT k, count() GROUP BY k)
)
ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_projection_alias_key_alter MODIFY COLUMN j JSON(x UInt64) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_projection_alias_key_alter MODIFY COLUMN t Tuple(x UInt8, y UInt8) SETTINGS allow_metadata_only_named_tuple_alter = 1;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_projection_alias_key_alter' ORDER BY position;

-- the check still finds a column that `WHERE` of such a projection reads
ALTER TABLE t_projection_alias_key_alter ADD PROJECTION p_where (SELECT a % 5 AS k, count() WHERE j.y = 1 GROUP BY k);
ALTER TABLE t_projection_alias_key_alter MODIFY COLUMN j JSON(x UInt64, y UInt64) SETTINGS enable_json_lazy_type_hints = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_projection_alias_key_alter;
