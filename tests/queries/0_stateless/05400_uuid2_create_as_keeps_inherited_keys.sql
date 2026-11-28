-- Tags: no-replicated-database
-- Tag no-replicated-database: `CREATE CLONE AS is not supported with Replicated databases`

-- `CREATE TABLE ... AS` and `CLONE AS` under `uuid_type_version = 2` inherit the storage definition of the source
-- table as it is stored: a bare `UUID` in its keys keeps denoting the historical `UUID` type.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET uuid_type_version = 1;
CREATE TABLE src (s String) ENGINE = MergeTree ORDER BY CAST(s, 'UUID');
INSERT INTO src VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');

SET uuid_type_version = 2;
CREATE TABLE dst_clone CLONE AS src;
CREATE TABLE dst_as AS src;
-- Clauses written in the query itself are still materialized.
CREATE TABLE dst_as_order AS src ORDER BY CAST(s, 'UUID');

SELECT name, sorting_key FROM system.tables WHERE database = currentDatabase() AND name LIKE 'dst%' ORDER BY name;
SELECT s FROM dst_clone;

DROP TABLE src;
DROP TABLE dst_clone;
DROP TABLE dst_as;
DROP TABLE dst_as_order;
