-- Regression test for arrayJoin() used as function with JOIN and WHERE (issue #96398).
-- arrayJoin() must be materialized once per source row before JOIN to avoid duplicated rows.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt8, arr Array(UInt8)) ENGINE=Memory;
CREATE TABLE t2 (id UInt8) ENGINE=Memory;

INSERT INTO t1 VALUES (1, [1,2]), (2, [3]);
INSERT INTO t2 VALUES (1), (2);

SELECT
    t1.id,
    arrayJoin(arr) AS x
FROM t1
INNER JOIN t2 USING (id)
WHERE x > 0
ORDER BY id, x;

DROP TABLE t1;
DROP TABLE t2;
