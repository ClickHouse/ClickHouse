-- A `joinGet` holds a lock on its table for as long as its function lives. The function must not outlive the
-- query that built it, or the table cannot be truncated afterwards.

DROP TABLE IF EXISTS t_join_get_lock;

CREATE TABLE t_join_get_lock (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_join_get_lock VALUES (1, 10);

SELECT joinGet('t_join_get_lock', 'v', toUInt64(1));

SET lock_acquire_timeout = 10;
TRUNCATE TABLE t_join_get_lock;
SELECT count() FROM t_join_get_lock;

DROP TABLE t_join_get_lock;
