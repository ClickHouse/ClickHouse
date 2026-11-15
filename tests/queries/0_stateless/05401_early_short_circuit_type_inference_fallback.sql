-- Early AND/OR short-circuit falls back to normal analysis when a count() scalar feeds an argument that must be constant
DROP TABLE IF EXISTS t_early_short_circuit_fallback;
CREATE TABLE t_early_short_circuit_fallback (x UInt8) ENGINE = Memory;
INSERT INTO t_early_short_circuit_fallback VALUES (1), (2);

SET enable_analyzer = 1;
SET enable_function_early_short_circuit = 1;

SELECT 1 OR tupleElement((10, 20), assumeNotNull((SELECT count() FROM t_early_short_circuit_fallback)));
SELECT 0 AND tupleElement((10, 20), assumeNotNull((SELECT count() FROM t_early_short_circuit_fallback)));

DROP TABLE t_early_short_circuit_fallback;
