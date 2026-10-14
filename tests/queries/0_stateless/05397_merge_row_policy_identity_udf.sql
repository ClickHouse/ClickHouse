-- A row policy whose filter is a SQL UDF that returns its argument unchanged resolves to the input column itself.
-- Reading through `Merge` must still request the physical column, not the expression name.

DROP TABLE IF EXISTS r_identity_05397;
DROP FUNCTION IF EXISTS identity_udf_05397;

CREATE TABLE r_identity_05397 (flag UInt8, v UInt8) ENGINE = MergeTree ORDER BY v;
INSERT INTO r_identity_05397 VALUES (0, 0), (1, 1), (0, 2), (1, 3);

CREATE FUNCTION identity_udf_05397 AS x -> x;
CREATE ROW POLICY policy_05397 ON r_identity_05397 USING identity_udf_05397(flag) TO ALL;

SELECT v FROM r_identity_05397 ORDER BY v;
SELECT v FROM merge(currentDatabase(), '^r_identity_05397$') ORDER BY v;
SELECT flag, v FROM merge(currentDatabase(), '^r_identity_05397$') ORDER BY v;

DROP ROW POLICY policy_05397 ON r_identity_05397;
DROP TABLE r_identity_05397;
DROP FUNCTION identity_udf_05397;
