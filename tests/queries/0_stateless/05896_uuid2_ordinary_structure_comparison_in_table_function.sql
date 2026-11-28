-- A table function argument of the form `structure = '...'` is a named schema argument only in the table functions
-- that take `key = value` arguments. In `numbers` it is an ordinary comparison (here with a `WITH` alias), and in
-- a parameterized view it is the value of an ordinary `String` parameter. Materializing `uuid_type_version` into
-- a persisted query must not rewrite it: the persisted query must return the same as the direct one.

SET uuid_type_version = 2;

DROP TABLE IF EXISTS t_numbers_structure;
DROP VIEW IF EXISTS v_numbers_structure;
DROP VIEW IF EXISTS v_structure_parameter_user;
DROP VIEW IF EXISTS v_structure_parameter;

SELECT 'numbers, direct';
WITH 'id UUID' AS structure SELECT count() FROM numbers(structure = 'id UUID');

CREATE TABLE t_numbers_structure ENGINE = Memory AS
    WITH 'id UUID' AS structure SELECT * FROM numbers(structure = 'id UUID');
SELECT 'numbers, table';
SELECT count() FROM t_numbers_structure;

CREATE VIEW v_numbers_structure AS
    WITH 'id UUID' AS structure SELECT * FROM numbers(structure = 'id UUID');
SELECT 'numbers, view';
SELECT count() FROM v_numbers_structure;

CREATE VIEW v_structure_parameter AS SELECT {structure:String} AS s;
SELECT 'parameterized view, direct';
SELECT s FROM v_structure_parameter(structure = 'id UUID');

CREATE VIEW v_structure_parameter_user AS SELECT s FROM v_structure_parameter(structure = 'id UUID');
SELECT 'parameterized view, view';
SELECT s FROM v_structure_parameter_user;

DROP VIEW v_structure_parameter_user;
DROP VIEW v_structure_parameter;
DROP VIEW v_numbers_structure;
DROP TABLE t_numbers_structure;
