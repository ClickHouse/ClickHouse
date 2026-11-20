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

-- An `ALTER` that does not define the TTL again keeps the codec accepted at `CREATE` or by `MODIFY TTL`, as it keeps an
-- unchanged column `CODEC`. A codec that the `ALTER` defines is still checked against the settings of the session.
SET enable_zxc_codec = 1, allow_suspicious_codecs = 1;
CREATE TABLE t_alter (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);
CREATE TABLE t_modify (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY d;
ALTER TABLE t_modify MODIFY TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(Delta);

SET enable_zxc_codec = 0, allow_suspicious_codecs = 0;
ALTER TABLE t_alter ADD COLUMN y UInt32;
ALTER TABLE t_modify ADD COLUMN y UInt32;
ALTER TABLE t_modify MODIFY TTL d + INTERVAL 2 DAY RECOMPRESS CODEC(Delta); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter MODIFY TTL d + INTERVAL 2 DAY RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }

SELECT name, extract(engine_full, 'TTL .*? SETTINGS') FROM system.tables
    WHERE database = currentDatabase() AND name IN ('t_alter', 't_modify') ORDER BY name;
SELECT table, groupArray(name) FROM (SELECT table, name FROM system.columns
    WHERE database = currentDatabase() AND table IN ('t_alter', 't_modify') ORDER BY table, position) GROUP BY table ORDER BY table;

DROP TABLE t_alter;
DROP TABLE t_modify;
