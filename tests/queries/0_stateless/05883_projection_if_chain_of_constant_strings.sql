-- a chain of `if` with constant strings stays `if`: `multiIf` would have the type `LowCardinality(String)`

DROP TABLE IF EXISTS t_projection_if_chain_strings;

SET optimize_if_chain_to_multiif = 1, optimize_if_transform_const_strings_to_lowcardinality = 1, optimize_if_transform_strings_to_enum = 0;

CREATE TABLE t_projection_if_chain_strings (a UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_if_chain_strings ADD PROJECTION p (SELECT count() GROUP BY if(a = 0, 'zero', if(a = 1, 'one', 'other')));

INSERT INTO t_projection_if_chain_strings SELECT number % 3 FROM numbers(30);

SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_if_chain_strings';

SELECT DISTINCT column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_chain_strings' AND active
ORDER BY column_position;

-- the table is loaded again and gets a part from a session without the string setting
DETACH TABLE t_projection_if_chain_strings;
ATTACH TABLE t_projection_if_chain_strings;

SET optimize_if_transform_const_strings_to_lowcardinality = 0;
INSERT INTO t_projection_if_chain_strings SELECT number % 3 FROM numbers(30);

SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_if_chain_strings';

SELECT DISTINCT column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_chain_strings' AND active
ORDER BY column_position;

SELECT if(a = 0, 'zero', if(a = 1, 'one', 'other')) AS k, count() FROM t_projection_if_chain_strings GROUP BY k ORDER BY k;

DROP TABLE t_projection_if_chain_strings;
