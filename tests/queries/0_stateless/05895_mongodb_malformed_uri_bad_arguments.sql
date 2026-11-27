-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: needs USE_MONGODB
-- Tag no-replicated-database: uses ON CLUSTER

CREATE TABLE t_uri (a String) ENGINE = MongoDB('not-a-uri', 'c'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_port (a String) ENGINE = MongoDB('mongodb://h:99999999/db', 'c'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_path (a String) ENGINE = MongoDB('localhost/ignored/path', 'db', 'c', 'u', 'p'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_as AS mongodb('not-a-uri', 'c', 'a String'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mongodb('not-a-uri', 'c', 'a String'); -- { serverError BAD_ARGUMENTS }

SET distributed_ddl_output_mode = 'throw';
CREATE TABLE t_on_cluster ON CLUSTER test_shard_localhost (a String) ENGINE = MongoDB('not-a-uri', 'c') FORMAT Null; -- { serverError BAD_ARGUMENTS }
