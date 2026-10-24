-- Tags: no-fasttest
-- no-fasttest: uses `icebergAzure`

-- `uuid_type_version = 2` is materialized into the expressions a `CREATE` persists, including the ones that an AST
-- node does not keep in its children: the key and the assignments of a TTL `GROUP BY`, an `APPLY` transformer, and
-- the declared types of the query parameters of a parameterized view.

SET uuid_type_version = 2;

DROP TABLE IF EXISTS t_uuid2_ttl_group_by;
CREATE TABLE t_uuid2_ttl_group_by (s String, d DateTime, x String)
ENGINE = MergeTree ORDER BY CAST(s, 'UUID')
TTL d GROUP BY CAST(s, 'UUID') SET x = toString(any(CAST(x, 'UUID')));
SELECT countSubstrings(create_table_query, 'UUID2'), countSubstrings(create_table_query, '\'UUID\'')
FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_ttl_group_by';
DROP TABLE t_uuid2_ttl_group_by;

DROP TABLE IF EXISTS t_uuid2_apply;
CREATE TABLE t_uuid2_apply ENGINE = Memory AS
SELECT * APPLY (x -> CAST(x, 'UUID')) FROM (SELECT '00000000-0000-0000-0000-000000000001' AS a);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_apply';
DROP TABLE t_uuid2_apply;

DROP VIEW IF EXISTS v_uuid2_parameters;
CREATE VIEW v_uuid2_parameters AS SELECT toTypeName({p:UUID}) AS p, toTypeName({q:Array(UUID)}) AS q, toTypeName({r:UUID1}) AS r;
SET uuid_type_version = 1;
SELECT * FROM v_uuid2_parameters(
    p = '00000000-0000-0000-0000-000000000001', q = ['00000000-0000-0000-0000-000000000001'], r = '00000000-0000-0000-0000-000000000001');
SET uuid_type_version = 2;
DROP VIEW v_uuid2_parameters;

-- An internal `CREATE` built from already resolved types (here, the inner table of a materialized view) keeps them.
DROP TABLE IF EXISTS mv_uuid2_inner;
CREATE MATERIALIZED VIEW mv_uuid2_inner (u UUID1) ENGINE = MergeTree ORDER BY u
AS SELECT toUUID('00000000-0000-0000-0000-000000000001') AS u;
SELECT DISTINCT type FROM system.columns WHERE database = currentDatabase() AND (table = 'mv_uuid2_inner' OR table LIKE '.inner%');
DROP TABLE mv_uuid2_inner;

-- The positional argument 2 of the Azure data-lake table functions is the blob path, which stays as is.
DROP TABLE IF EXISTS t_uuid2_azure_path;
CREATE TABLE t_uuid2_azure_path (k UInt8) AS icebergAzure('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 'id UUID', 'CSV', 'k UUID');
SELECT position(create_table_query, 'UUID2') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_azure_path';
DROP TABLE t_uuid2_azure_path;
