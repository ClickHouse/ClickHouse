-- A lambda declared in the query shadows a SQL UDF of the same name called from the body of another UDF in `APPLY`.

DROP FUNCTION IF EXISTS f_05617_g;
DROP FUNCTION IF EXISTS f_05617_f;
CREATE FUNCTION f_05617_g AS x -> x + 1;
CREATE FUNCTION f_05617_f AS x -> tuple(f_05617_g(x), CAST('00000000-0000-0000-0000-000000000001', 'UUID'));

SET uuid_type_version = 2;

WITH (x -> x * 3) AS f_05617_g
SELECT * APPLY f_05617_f FROM values('a UInt8', 1);

DROP TABLE IF EXISTS t_05617;
CREATE TABLE t_05617 ENGINE = Memory AS
WITH (x -> x * 3) AS f_05617_g
SELECT * APPLY f_05617_f FROM values('a UInt8', 1);
SELECT * FROM t_05617;
DROP TABLE t_05617;

-- Without a lambda of that name, the UDF applies.
SELECT * APPLY f_05617_f FROM values('a UInt8', 1);

DROP FUNCTION f_05617_f;
DROP FUNCTION f_05617_g;

-- An explicit `UUID1` keeps the historical type in `ALTER` with `data_type_default_nullable`.
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_05617_alter;
CREATE TABLE t_05617_alter (x UInt8) ENGINE = Memory;
SET uuid_type_version = 2, data_type_default_nullable = 1;
ALTER TABLE t_05617_alter ADD COLUMN u UUID1;
ALTER TABLE t_05617_alter ADD COLUMN v UUID;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_05617_alter' ORDER BY name;
ALTER TABLE t_05617_alter MODIFY COLUMN u UUID1;
ALTER TABLE t_05617_alter MODIFY COLUMN v UUID1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_05617_alter' ORDER BY name;
DROP TABLE t_05617_alter;
