DROP TABLE IF EXISTS t_niv_mul;
CREATE TABLE t_niv_mul (idx UInt32, val Float64, grp UInt8) ENGINE = Memory;
INSERT INTO t_niv_mul VALUES (1, 10, 1), (2, 20, 1), (3, 30, 1), (2, 1, 2), (3, 1, 2), (4, 1, 2);

SELECT 'same_frac', numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(
    (SELECT groupNumericIndexedVectorState('BSI', 40, 24)(idx, val) FROM t_niv_mul WHERE grp = 1),
    (SELECT groupNumericIndexedVectorState('BSI', 40, 24)(idx, val) FROM t_niv_mul WHERE grp = 2)
));

SELECT 'diff_frac', numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(
    (SELECT groupNumericIndexedVectorState('BSI', 40, 24)(idx, val) FROM t_niv_mul WHERE grp = 1),
    (SELECT groupNumericIndexedVectorState('BSI', 40, 0)(idx, val) FROM t_niv_mul WHERE grp = 2)
));

DROP TABLE IF EXISTS t_niv_mul;
