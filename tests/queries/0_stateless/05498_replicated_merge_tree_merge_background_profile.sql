-- Tags: zookeeper
-- Merges run with the settings of the background profile, which sets `custom_background_profile_marker`
-- (tests/config/users.d/background_profile.xml). A merge that can remove rows rebuilds the aggregate projection
-- with its own settings, so the projection keeps the marker of the merge.

SET custom_background_profile_marker = 'query';

DROP TABLE IF EXISTS t_mt_background_profile;
DROP TABLE IF EXISTS t_rmt_background_profile;

CREATE TABLE t_mt_background_profile
(
    x UInt64,
    v UInt64,
    PROJECTION p (SELECT x, max(concat(toString(getSettingOrDefault('custom_background_profile_marker', 'none')), '_', toString(v))) GROUP BY x)
)
ENGINE = ReplacingMergeTree(v) ORDER BY x
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE t_rmt_background_profile
(
    x UInt64,
    v UInt64,
    PROJECTION p (SELECT x, max(concat(toString(getSettingOrDefault('custom_background_profile_marker', 'none')), '_', toString(v))) GROUP BY x)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/t_rmt_background_profile', '1', v) ORDER BY x
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_mt_background_profile VALUES (1, 1);
INSERT INTO t_mt_background_profile VALUES (1, 2);
INSERT INTO t_rmt_background_profile VALUES (1, 1);
INSERT INTO t_rmt_background_profile VALUES (1, 2);

OPTIMIZE TABLE t_mt_background_profile FINAL;
OPTIMIZE TABLE t_rmt_background_profile FINAL;

SELECT 'MergeTree', finalizeAggregation(COLUMNS('^max')) FROM mergeTreeProjection(currentDatabase(), t_mt_background_profile, p);
SELECT 'ReplicatedMergeTree', finalizeAggregation(COLUMNS('^max')) FROM mergeTreeProjection(currentDatabase(), t_rmt_background_profile, p);

DROP TABLE t_mt_background_profile;
DROP TABLE t_rmt_background_profile;
