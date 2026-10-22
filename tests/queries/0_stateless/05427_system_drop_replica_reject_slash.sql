-- Tags: no-parallel

SYSTEM DROP REPLICA 'r/1' FROM TABLE t_does_not_exist; -- { serverError BAD_ARGUMENTS }
SYSTEM DROP DATABASE REPLICA 'r/1' FROM DATABASE db_does_not_exist WITH TABLES; -- { serverError BAD_ARGUMENTS }
