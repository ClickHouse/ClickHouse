-- The codec of `TTL ... RECOMPRESS` in `CREATE TABLE` is gated by the settings of the session, like a column `CODEC`
-- and like `ALTER TABLE ... MODIFY TTL`. It used to be checked against the server-wide settings only, so it was refused
-- with a message asking to enable the setting that was already enabled.

SET enable_zxc_codec = 0, allow_suspicious_codecs = 0;

CREATE TABLE t_zxc (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_delta (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(Delta); -- { serverError BAD_ARGUMENTS }

SET enable_zxc_codec = 1;
CREATE TABLE t_zxc (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);

SET allow_suspicious_codecs = 1;
CREATE TABLE t_delta (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(Delta);

-- The tables load with the server-wide settings.
SET enable_zxc_codec = 0, allow_suspicious_codecs = 0;
DETACH TABLE t_zxc;
ATTACH TABLE t_zxc;
DETACH TABLE t_delta;
ATTACH TABLE t_delta;

INSERT INTO t_zxc VALUES ('2020-01-01 00:00:00', 1);
INSERT INTO t_delta VALUES ('2020-01-01 00:00:00', 2);
SELECT x FROM t_zxc;
SELECT x FROM t_delta;
SELECT name, extract(engine_full, 'TTL .*? SETTINGS') FROM system.tables WHERE database = currentDatabase() ORDER BY name;

DROP TABLE t_zxc;
DROP TABLE t_delta;
