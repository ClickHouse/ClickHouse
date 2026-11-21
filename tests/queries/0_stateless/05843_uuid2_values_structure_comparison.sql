-- In the `values` table function, `structure = '...'` is an ordinary comparison (here with a `WITH` alias),
-- not a named schema argument. Materializing `uuid_type_version` into a persisted query must not rewrite it.

SET uuid_type_version = 2;

DROP TABLE IF EXISTS t_values_structure;
DROP VIEW IF EXISTS v_values_structure;

SELECT 'direct';
WITH 'id UUID' AS structure SELECT * FROM values('x UInt8', structure = 'id UUID');

CREATE TABLE t_values_structure ENGINE = Memory AS
    WITH 'id UUID' AS structure SELECT * FROM values('x UInt8', structure = 'id UUID');
SELECT 'table';
SELECT * FROM t_values_structure;

CREATE VIEW v_values_structure AS
    WITH 'id UUID' AS structure SELECT * FROM values('x UInt8', structure = 'id UUID');
SELECT 'view';
SELECT * FROM v_values_structure;

-- The schema of `values` is still materialized.
DROP VIEW v_values_structure;
CREATE VIEW v_values_structure AS SELECT * FROM values('id UUID', '00000000-0000-0000-0000-000000000001');
SELECT 'schema';
SELECT toTypeName(id), id FROM v_values_structure;

DROP VIEW v_values_structure;
DROP TABLE t_values_structure;
