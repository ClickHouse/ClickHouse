-- A projection text index has a fixed output schema (`term`, `posting`), which does not change when a source
-- column changes its type. A metadata-only conversion changes the values of the indexed expression without
-- rebuilding the index, so the stale index must not be used to prune rows.

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_proj_text_alter;

CREATE TABLE t_proj_text_alter
(
    id UInt64,
    ts UInt32,
    u UInt8,
    s String,
    PROJECTION p_ts INDEX toString(ts) TYPE text(tokenizer = splitByNonAlpha),
    PROJECTION p_u INDEX toString(u) TYPE text(tokenizer = splitByNonAlpha),
    PROJECTION p_s INDEX s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_proj_text_alter VALUES (1, 1, 1, 'a b'), (2, 2, 0, 'c');

ALTER TABLE t_proj_text_alter MODIFY COLUMN ts DateTime('UTC');
SELECT count() FROM t_proj_text_alter WHERE hasToken(toString(ts), '1970');
SELECT count() FROM t_proj_text_alter WHERE hasToken(toString(ts), '1970') SETTINGS use_skip_indexes = 0;

ALTER TABLE t_proj_text_alter MODIFY COLUMN u Bool;
SELECT count() FROM t_proj_text_alter WHERE hasToken(toString(u), 'true');
SELECT count() FROM t_proj_text_alter WHERE hasToken(toString(u), 'true') SETTINGS use_skip_indexes = 0;

-- An unchanged source column keeps using its index.
SELECT count() FROM t_proj_text_alter WHERE hasToken(s, 'a');

DROP TABLE t_proj_text_alter;
