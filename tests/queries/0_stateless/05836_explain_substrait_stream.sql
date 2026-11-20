z-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_stream;
CREATE TABLE t_substrait_stream (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_stream VALUES (1);

EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_stream STREAM SETTINGS enable_streaming_queries = 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_stream;
