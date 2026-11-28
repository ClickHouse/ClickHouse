-- The BFloat16 L2 distance must not cancel catastrophically for close vectors with large components,
-- or overflow for identical vectors with huge components.

DROP TABLE IF EXISTS qbit;
CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 2)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [32768, 1]), (2, [1180591620717411303424, 0]);

SELECT id, L2DistanceTransposed(vec, [32768., 2.], 16) FROM qbit WHERE id = 1;
SELECT id, L2DistanceTransposed(vec, [1180591620717411303424., 0.], 16) FROM qbit WHERE id = 2;

DROP TABLE qbit;
