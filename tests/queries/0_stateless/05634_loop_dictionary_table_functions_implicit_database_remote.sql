-- Tags: shard

-- `loop` and `dictionary` with a name without a database read the current database of the query, also when the query is sent
-- to other servers, whose own default database is different.

CREATE TABLE t_loop_dict_implicit_db (x UInt64, v UInt8) ENGINE = Memory;
INSERT INTO t_loop_dict_implicit_db VALUES (2, 0), (3, 0);
CREATE DICTIONARY d_loop_dict_implicit_db (x UInt64, v UInt8) PRIMARY KEY x
    SOURCE(CLICKHOUSE(TABLE 't_loop_dict_implicit_db')) LIFETIME(0) LAYOUT(HASHED());

SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(t_loop_dict_implicit_db) LIMIT 2);
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop('', t_loop_dict_implicit_db) LIMIT 2);
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM dictionary('d_loop_dict_implicit_db'));
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM dictionary(d_loop_dict_implicit_db));

-- The condition is inside an aggregate function, evaluated on two shards.
SELECT countIf(number IN (SELECT x FROM loop(t_loop_dict_implicit_db) LIMIT 2)) FROM remote('127.0.0.{2,3}', numbers(4));
SELECT countIf(number IN (SELECT x FROM dictionary('d_loop_dict_implicit_db'))) FROM remote('127.0.0.{2,3}', numbers(4));

SELECT sum(l.x) FROM remote('127.0.0.2', numbers(4)) AS r INNER JOIN (SELECT x FROM loop(t_loop_dict_implicit_db) LIMIT 2) AS l ON r.number = l.x;
SELECT sum(number), count() FROM remote('127.0.0.2', numbers(4)) WHERE number IN (SELECT x FROM loop(dictionary('d_loop_dict_implicit_db')) LIMIT 2);

INSERT INTO FUNCTION file(currentDatabase() || '_05634.tsv', 'TSV', 'n UInt64') SELECT number FROM numbers(4)
    SETTINGS engine_file_truncate_on_insert = 1;
SELECT sum(n), count()
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05634.tsv', 'TSV', 'n UInt64')
WHERE n IN (SELECT x FROM loop(t_loop_dict_implicit_db) LIMIT 2);

-- The table of remote() is resolved by the remote server, in its own default database.
SELECT x FROM remote('127.0.0.2', loop(t_loop_dict_implicit_db)) LIMIT 1; -- { serverError UNKNOWN_TABLE }
-- A table with its own columns skips the structure lookup, so only reading it resolves the table of remote().
CREATE TABLE r_loop_dict_implicit_db_loop (x UInt64) AS remote('127.0.0.2', loop(t_loop_dict_implicit_db));
SELECT x FROM r_loop_dict_implicit_db_loop LIMIT 1; -- { serverError UNKNOWN_TABLE }
SELECT x FROM r_loop_dict_implicit_db_loop LIMIT 1 SETTINGS serialize_query_plan = 1; -- { serverError UNKNOWN_TABLE }
CREATE TABLE r_loop_dict_implicit_db_merge (x UInt64) AS remote('127.0.0.2', merge('^t_loop_dict_implicit_db$'));
SELECT x FROM r_loop_dict_implicit_db_merge LIMIT 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT x FROM r_loop_dict_implicit_db_merge LIMIT 1 SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
