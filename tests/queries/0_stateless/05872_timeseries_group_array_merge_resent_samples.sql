-- Samples of a series sent again into another part: merging the rows keeps one sample per timestamp, the greatest value wins.
DROP TABLE IF EXISTS ts_resent_samples;
CREATE TABLE ts_resent_samples
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY id;

SYSTEM STOP MERGES ts_resent_samples;
INSERT INTO ts_resent_samples VALUES (1, [('2026-01-01 00:00:00', 1.), ('2026-01-01 00:00:15', 5.), ('2026-01-01 00:00:30', 3.)]), (2, [('2026-01-01 00:00:00', 7.)]);
INSERT INTO ts_resent_samples VALUES (1, [('2026-01-01 00:00:15', 2.), ('2026-01-01 00:00:30', 4.), ('2026-01-01 00:00:45', 6.)]), (2, [('2026-01-01 00:00:15', 8.)]);

SELECT id, samples FROM ts_resent_samples FINAL ORDER BY id;

SYSTEM START MERGES ts_resent_samples;
OPTIMIZE TABLE ts_resent_samples FINAL;
SELECT id, samples FROM ts_resent_samples ORDER BY id;

DROP TABLE ts_resent_samples;
