#!/bin/bash
set -e

WORK_TREE="$1"
(
  cd $WORK_TREE || exit 1

  echo "$PWD"

  cat << EOF > /tmp/query.sql
SET enable_analyzer=1;
SET query_plan_merge_filters = 0;

SELECT version();

DROP FUNCTION IF EXISTS het2Bytes;
DROP FUNCTION IF EXISTS unhexPrefixed;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE FUNCTION het2Bytes AS (x) -> CAST(unhexPrefixed(x), 'FixedString(20)');
CREATE FUNCTION unhexPrefixed AS VALUE -> unhex(substr(VALUE, 3));

CREATE TABLE t ENGINE=MergeTree ORDER BY tuple() AS SELECT arrayJoin(['4D7953514C', 'выьдыьдыдлтьыфвдльждлцьуфлоьфьс']) AS s;
CREATE TABLE t2 ENGINE=MergeTree ORDER BY tuple() AS SELECT het2Bytes('4D7953514C') AS s;
CREATE TABLE t3 ENGINE=MergeTree ORDER BY (c, s) AS SELECT s, s LIKE '4%' AS c FROM t;

SELECT * FROM t;
SELECT * FROM t2;
SELECT * FROM t3;

WITH cte1 AS
  (SELECT t3.s AS s3, t2.s AS s2, t3.c AS c FROM t3 INNER JOIN t2 ON het2Bytes(t3.s) = t2.s WHERE t3.c)
SELECT 'OK', * FROM cte1;

WITH cte1 AS
  (SELECT t3.s AS s3, t2.s AS s2, t3.c AS c FROM t3 INNER JOIN t2 ON het2Bytes(t3.s) = t2.s WHERE t3.c)
SELECT * FROM cte1 WHERE s2 IN (SELECT DISTINCT s2 FROM cte1);
EOF

  # CH_PATH is the path to downloaded binary
  ! $CH_PATH client --queries-file /tmp/query.sql 2>&1
)
