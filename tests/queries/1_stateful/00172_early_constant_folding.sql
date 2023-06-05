-- Tags: no-parallel-replicas

set allow_experimental_analyzer=0;

set max_threads=10;
EXPLAIN PIPELINE SELECT count(JavaEnable) FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1;
