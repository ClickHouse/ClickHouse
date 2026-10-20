-- Tags: no-parallel

SYSTEM DROP REPLICA 'r/1' FROM TABLE t_does_not_exist; -- { serverError BAD_ARGUMENTS }
