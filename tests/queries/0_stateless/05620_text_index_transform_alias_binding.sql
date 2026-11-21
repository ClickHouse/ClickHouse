DROP TABLE IF EXISTS t_pre;
DROP TABLE IF EXISTS t_post;

CREATE TABLE t_pre
(
    id UInt64,
    val String,
    lowered String ALIAS lower(val),
    transformed String ALIAS lowered,
    INDEX idx val TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = transformed)
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_pre;
INSERT INTO t_pre (id, val) VALUES (1, 'Hello');

-- Reload the stored definition before changing an indirect `ALIAS` dependency.
DETACH TABLE t_pre;
ATTACH TABLE t_pre;
SYSTEM STOP MERGES t_pre;
ALTER TABLE t_pre MODIFY COLUMN lowered String ALIAS upper(val);
INSERT INTO t_pre (id, val) VALUES (2, 'HELLO');

SELECT arraySort(groupArray(id)) FROM t_pre WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM t_pre WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 0;

-- Bound transforms no longer depend on the `ALIAS` columns, even after another reload.
ALTER TABLE t_pre DROP COLUMN transformed, DROP COLUMN lowered;
DETACH TABLE t_pre;
ATTACH TABLE t_pre;
SELECT arraySort(groupArray(id)) FROM t_pre WHERE hasToken(val, 'HELLO')
SETTINGS force_data_skipping_indices = 'idx';

SYSTEM START MERGES t_pre;
OPTIMIZE TABLE t_pre FINAL;
SELECT arraySort(groupArray(id)) FROM t_pre WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_pre;

CREATE TABLE t_post
(
    id UInt64,
    val String,
    lowered String ALIAS lower(val),
    transformed String ALIAS lowered
)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_post ADD INDEX idx val TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = transformed);
SYSTEM STOP MERGES t_post;
INSERT INTO t_post (id, val) VALUES (1, 'Hello');
ALTER TABLE t_post MODIFY COLUMN lowered String ALIAS upper(val);
INSERT INTO t_post (id, val) VALUES (2, 'HELLO');

SELECT arraySort(groupArray(id)) FROM t_post WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM t_post WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 0;

ALTER TABLE t_post DROP COLUMN transformed, DROP COLUMN lowered;
DETACH TABLE t_post;
ATTACH TABLE t_post;
SELECT arraySort(groupArray(id)) FROM t_post WHERE hasToken(val, 'HELLO')
SETTINGS force_data_skipping_indices = 'idx';

SYSTEM START MERGES t_post;
OPTIMIZE TABLE t_post FINAL;
SELECT arraySort(groupArray(id)) FROM t_post WHERE hasToken(val, 'hello')
SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_post;
