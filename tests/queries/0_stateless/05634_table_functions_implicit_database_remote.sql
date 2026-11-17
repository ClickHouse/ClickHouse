-- Tags: shard, no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` and `prometheusQuery` need ANTLR4, which the fast-test build does not have.

-- A table function that reads the current database, through a name without a database or through '', reads it also when the
-- query is sent to other servers, whose own default database is different.

SET allow_experimental_time_series_table = 1;
SET enable_full_text_index = 1;

CREATE TABLE t_implicit_db (x UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha), PROJECTION p (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x;
INSERT INTO t_implicit_db VALUES (2, 'a'), (3, 'b');
CREATE DICTIONARY d_implicit_db (x UInt64, s String) PRIMARY KEY x
    SOURCE(CLICKHOUSE(TABLE 't_implicit_db')) LIFETIME(0) LAYOUT(HASHED());
CREATE TABLE ts_implicit_db ENGINE = TimeSeries;
INSERT INTO ts_implicit_db (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.)]);

-- A table name without a database, '' and a dictionary name, in a subquery of IN and of JOIN.
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(t_implicit_db) LIMIT 2);
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM mergeTreeProjection('', t_implicit_db, p));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM dictionary('d_implicit_db'));
SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN (SELECT x FROM loop(t_implicit_db) LIMIT 2) AS l ON r.number = l.x;
SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN (SELECT x FROM mergeTreeProjection('', t_implicit_db, p)) AS l ON r.number = l.x;
SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN (SELECT x FROM dictionary('d_implicit_db')) AS l ON r.number = l.x;

-- A call inside another table function, and a query sent by a *Cluster table function.
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(dictionary('d_implicit_db')) LIMIT 2);
INSERT INTO FUNCTION file(currentDatabase() || '_05634.tsv', 'TSV', 'n UInt64') SELECT number FROM numbers(4)
    SETTINGS engine_file_truncate_on_insert = 1;
SELECT sum(n), count()
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05634.tsv', 'TSV', 'n UInt64')
WHERE n IN (SELECT x FROM loop(t_implicit_db) LIMIT 2);

-- The other table functions that read the current database.
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM mergeTreeIndex('', t_implicit_db));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM mergeTreeAnalyzeIndexes('', t_implicit_db));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM mergeTreeCodecBlockCounts('', t_implicit_db));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM mergeTreeTextIndex('', t_implicit_db, idx));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM timeSeriesSamples(ts_implicit_db));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM timeSeriesSelector(ts_implicit_db, 'up', 0, 1000, 'samples'));
SELECT count() FROM remote('127.0.0.2', numbers(1)) WHERE number IN (SELECT count() * 0 FROM prometheusQuery(ts_implicit_db, 'up', 1000));

-- Parallel replicas are sent the query as well.
SELECT metric_name FROM timeSeriesTags(ts_implicit_db)
    SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, automatic_parallel_replicas_mode = 0;

-- The table of remote() is resolved by the remote server, in its own default database.
SELECT x FROM remote('127.0.0.2', loop(t_implicit_db)) LIMIT 1; -- { serverError UNKNOWN_TABLE }
-- A table with its own columns skips the structure lookup, so only reading it resolves the table of remote().
CREATE TABLE r_implicit_db_loop (x UInt64) AS remote('127.0.0.2', loop(t_implicit_db));
SELECT x FROM r_implicit_db_loop LIMIT 1; -- { serverError UNKNOWN_TABLE }
SELECT x FROM r_implicit_db_loop LIMIT 1 SETTINGS serialize_query_plan = 1; -- { serverError UNKNOWN_TABLE }
CREATE TABLE r_implicit_db_merge (x UInt64) AS remote('127.0.0.2', merge('^t_implicit_db$'));
SELECT x FROM r_implicit_db_merge LIMIT 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT x FROM r_implicit_db_merge LIMIT 1 SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
