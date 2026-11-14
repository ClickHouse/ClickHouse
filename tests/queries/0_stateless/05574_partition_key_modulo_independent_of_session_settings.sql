-- Tags: no-parallel

-- A table's partition key expression containing modulo (%) must be evaluated consistently with the types
-- defined by the table (in the global context), independent of session settings such as
-- to_start_of_interval_preserves_argument_type or compatibility.

DROP TABLE IF EXISTS t_mod_part;

CREATE TABLE t_mod_part (ts DateTime('UTC'), id UInt8)
ENGINE = MergeTree PARTITION BY (toStartOfInterval(ts, INTERVAL 1 MONTH), id % 2) ORDER BY ts;

INSERT INTO t_mod_part VALUES ('2024-03-15 10:00:00', 1);

-- SELECT with session setting changing toStartOfInterval return type must not fail or prune erroneously
SELECT count() FROM t_mod_part WHERE ts <= '2024-03-31 00:00:00' SETTINGS to_start_of_interval_preserves_argument_type = 0;

-- INSERT with session setting must not corrupt the stored partition value
INSERT INTO t_mod_part SETTINGS to_start_of_interval_preserves_argument_type = 0 VALUES ('2024-03-20 10:00:00', 1);

SELECT _partition_value, count() FROM t_mod_part GROUP BY ALL ORDER BY ALL;

SELECT count() FROM t_mod_part WHERE ts >= '2024-03-01 00:00:00';

DROP TABLE t_mod_part;
