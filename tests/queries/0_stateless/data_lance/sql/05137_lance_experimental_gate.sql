SET allow_experimental_lance = 0;
SELECT * FROM lanceLocal('missing.lance'); -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM lanceS3('http://localhost:1/missing.lance'); -- { serverError SUPPORT_IS_DISABLED }
SELECT * FROM lanceS3Cluster('missing_cluster', 'http://localhost:1/missing.lance'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE lance_gate_disabled (id UInt64) ENGINE = LanceLocal('missing.lance'); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE lance_gate_disabled (id UInt64) ENGINE = LanceS3('http://localhost:1/missing.lance'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_lance = 1;
SELECT count() FROM lanceLocal('tests/queries/0_stateless/data_lance/basic.lance');
CREATE TABLE lance_gate_enabled ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance');
SELECT count() FROM lance_gate_enabled;

SET allow_experimental_lance = 0;
DETACH TABLE lance_gate_enabled;
ATTACH TABLE lance_gate_enabled;
SELECT count() FROM lance_gate_enabled;
DROP TABLE lance_gate_enabled;
