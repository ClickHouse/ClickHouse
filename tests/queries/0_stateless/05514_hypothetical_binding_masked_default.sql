-- a change of a bound default makes the hypothetical index not applicable also when `query_masking_rules` hide the changed part
-- the test server config replaces `TOPSECRET.TOPSECRET` with `[hidden]`

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_masked_default;
CREATE TABLE t_masked_default (a UInt64, c UInt64 ALIAS if(position('TOPSECRET1TOPSECRET', '1') > 0, a, cityHash64(a)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_masked_default SELECT number FROM numbers(1000);

CREATE HYPOTHETICAL INDEX i ON t_masked_default (c) TYPE minmax;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_masked_default WHERE a = 100) WHERE explain LIKE '  status:%';

ALTER TABLE t_masked_default MODIFY COLUMN c UInt64 ALIAS if(position('TOPSECRET2TOPSECRET', '1') > 0, a, cityHash64(a));
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_masked_default WHERE a = 100) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

DROP TABLE t_masked_default;
