-- Tags: no-fasttest
-- no-fasttest: the S3 backup destination is not available in the fast test build.

-- The S3 locator of a `Backup` database is logged before it is validated. A `headers(...)` map must not make a
-- locator with two positionals look like the valid (url, access_key_id, secret_access_key) triple, so the value
-- after the url stays hidden, as it does for `BACKUP ... TO`.
CREATE DATABASE db_05497_hdr ENGINE = Backup('src', S3('http://localhost:11111/test/05497', 'SEKRIT_HDR',
    headers('a' = 'zz'))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE DATABASE db_05497_hdr_ec ENGINE = Backup('src', S3('http://localhost:11111/test/05497', 'SEKRIT_HDR_EC',
    headers('a' = 'zz'), extra_credentials(role_arn = 'r'))); -- { serverError BAD_ARGUMENTS }

-- Already hidden before: the same locator in `BACKUP ... TO`, and with `extra_credentials(...)` instead of `headers(...)`.
BACKUP TABLE system.one TO S3('http://localhost:11111/test/05497', 'SEKRIT_BKP',
    headers('a' = 'zz')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE DATABASE db_05497_ec ENGINE = Backup('src', S3('http://localhost:11111/test/05497', 'SEKRIT_EC',
    extra_credentials(role_arn = 'r'))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SYSTEM FLUSH LOGS query_log;

SELECT query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query_kind != 'Set'
  AND query NOT ILIKE 'SYSTEM FLUSH%'
  AND query_id = initial_query_id
  AND event_date >= yesterday() AND event_time >= now() - 600
ORDER BY event_time_microseconds;
