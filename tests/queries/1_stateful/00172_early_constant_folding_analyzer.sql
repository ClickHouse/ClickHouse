-- Tags: no-parallel-replicas

-- { echoOn }
set allow_experimental_analyzer=1;
set optimize_trivial_count_query=1;

set max_threads=10;
EXPLAIN header=1 SELECT count() FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1;
EXPLAIN PIPELINE header=1 SELECT count() FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1;
EXPLAIN header=1 SELECT count(JavaEnable) FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1;
EXPLAIN PIPELINE SELECT count(JavaEnable) FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1;
