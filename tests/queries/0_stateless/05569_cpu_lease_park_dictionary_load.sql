-- Tags: no-parallel
-- no-parallel: the server allows only one CPU resource at a time

DROP DICTIONARY IF EXISTS d_05569;
DROP VIEW IF EXISTS v_05569;
DROP TABLE IF EXISTS t_05569;
DROP WORKLOAD IF EXISTS 05569_one;
DROP WORKLOAD IF EXISTS 05569_all;
DROP RESOURCE IF EXISTS 05569_cpu;

CREATE RESOURCE 05569_cpu (CONSUME CPU);
CREATE WORKLOAD 05569_all;
CREATE WORKLOAD 05569_one IN 05569_all SETTINGS max_concurrent_threads = 1;

CREATE TABLE t_05569 (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05569 SELECT number, toString(number) FROM numbers(500000);

-- The aggregation makes the source query use several CPU quanta before it returns its first block
CREATE VIEW v_05569 AS SELECT k, any(v) AS v FROM t_05569 GROUP BY k;
CREATE DICTIONARY d_05569 (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'v_05569'))
    LAYOUT(HASHED()) LIFETIME(0) SETTINGS(dictionary_use_async_executor = 0);

-- The lookup loads the dictionary on another thread, in the workload whose only CPU slot this query holds while it waits
SELECT count() FROM numbers(1) WHERE dictHas('d_05569', number + 1)
SETTINGS workload = '05569_one', max_threads = 1, use_concurrency_control = 1, max_execution_time = 15;

DROP DICTIONARY d_05569;
DROP VIEW v_05569;
DROP TABLE t_05569;
DROP WORKLOAD 05569_one;
DROP WORKLOAD 05569_all;
DROP RESOURCE 05569_cpu;
