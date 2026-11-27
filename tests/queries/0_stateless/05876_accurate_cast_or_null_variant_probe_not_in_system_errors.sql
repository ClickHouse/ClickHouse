-- https://github.com/ClickHouse/ClickHouse/issues/134593: a failed conversion probe of an alternative must not be recorded in system.errors.

DROP TABLE IF EXISTS probe_qids;
DROP TABLE IF EXISTS variant_src;
DROP TABLE IF EXISTS dynamic_src;

CREATE TABLE probe_qids (name String, qid String, res Nullable(String)) ENGINE = Memory;
CREATE TABLE variant_src (v Variant(Array(UInt8), UInt64)) ENGINE = Memory;
INSERT INTO variant_src SELECT [1, 2]::Array(UInt8);
INSERT INTO variant_src SELECT 42::UInt64;
CREATE TABLE dynamic_src (d Dynamic) ENGINE = Memory;
INSERT INTO dynamic_src SELECT [1, 2]::Array(UInt8);
INSERT INTO dynamic_src SELECT 42::UInt64;

-- The Array alternative has no values (the query from the issue).
INSERT INTO probe_qids SELECT 'variant constant', queryID(), toString(accurateCastOrNull(CAST(toUInt64(1), 'Variant(Array(UInt8), UInt64)'), 'UInt64'));
SELECT 'variant constant', groupArray(name) FROM system.errors WHERE query_id IN (SELECT qid FROM probe_qids WHERE name = 'variant constant');

-- The Array alternative has a value, which becomes NULL.
INSERT INTO probe_qids SELECT 'variant column', queryID(), toString(accurateCastOrNull(v, 'UInt64')) FROM variant_src;
SELECT 'variant column', groupArray(name) FROM system.errors WHERE query_id IN (SELECT qid FROM probe_qids WHERE name = 'variant column');

-- Dynamic probes every type it stores.
INSERT INTO probe_qids SELECT 'dynamic column', queryID(), toString(accurateCastOrNull(d, 'UInt64')) FROM dynamic_src;
SELECT 'dynamic column', groupArray(name) FROM system.errors WHERE query_id IN (SELECT qid FROM probe_qids WHERE name = 'dynamic column');

SELECT name, res FROM probe_qids ORDER BY name, res;

DROP TABLE probe_qids;
DROP TABLE variant_src;
DROP TABLE dynamic_src;
