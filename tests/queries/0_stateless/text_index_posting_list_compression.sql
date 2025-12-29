SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_comp;

CREATE TABLE tab_comp
(
    _ts DateTime,
    _content String,
    INDEX inv_idx _content TYPE text(
        tokenizer = 'splitByNonAlpha',
        enable_posting_list_compression = true
    )
)
ENGINE = MergeTree
PARTITION BY toDate(_ts)
ORDER BY _ts;

DROP TABLE IF EXISTS tab_nocomp;

CREATE TABLE tab_nocomp
(
    _ts DateTime,
    _content String,
    INDEX inv_idx _content TYPE text(
        tokenizer = 'splitByNonAlpha',
        enable_posting_list_compression = false
    )
)
ENGINE = MergeTree
PARTITION BY toDate(_ts)
ORDER BY _ts;

INSERT INTO tab_comp
SELECT
    now() - 10 AS _ts,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS _content
FROM numbers(1024000);

INSERT INTO tab_nocomp
SELECT
    now() - 10 AS _ts,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS _content
FROM numbers(1024000);

INSERT INTO tab_comp
SELECT
    now() - 8 AS _ts,
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS _content
FROM numbers(512);

INSERT INTO tab_nocomp
SELECT
    now() - 8 AS _ts,
    multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS _content
FROM numbers(512);

INSERT INTO tab_comp
SELECT
    now() - 7 AS _ts,
    if(number < 1003, 'mid1003', 'noise') AS _content
FROM numbers(1500);

INSERT INTO tab_nocomp
SELECT
    now() - 7 AS _ts,
    if(number < 1003, 'mid1003', 'noise') AS _content
FROM numbers(1500);

INSERT INTO tab_comp
SELECT
    now() - 6 AS _ts,
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS _content
FROM numbers(2000);

INSERT INTO tab_nocomp
SELECT
    now() - 6 AS _ts,
    multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS _content
FROM numbers(2000);

OPTIMIZE TABLE tab_comp FINAL;

OPTIMIZE TABLE tab_nocomp FINAL;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'aa')) AS comp_aa,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'aa')) AS nocomp_aa,
    (comp_aa = nocomp_aa) AS ok_aa;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'tail129')) AS comp_tail129,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'tail129')) AS nocomp_tail129,
    comp_tail129, nocomp_tail129,
    (comp_tail129 = 129) AS ok_tail129,
    (comp_tail129 = nocomp_tail129) AS ok_tail129_eq;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'mid1003')) AS comp_mid1003,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'mid1003')) AS nocomp_mid1003,
    (comp_mid1003 = 1003) AS ok_mid1003,
    (comp_mid1003 = nocomp_mid1003) AS ok_mid1003_eq;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'single')) AS comp_single,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'single')) AS nocomp_single,
    (comp_single = 1) AS ok_single,
    (comp_single = nocomp_single) AS ok_single_eq;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'rare2')) AS comp_rare2,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'rare2')) AS nocomp_rare2,
    (comp_rare2 = 2) AS ok_rare2,
    (comp_rare2 = nocomp_rare2) AS ok_rare2_eq;

SELECT
    (SELECT count() FROM tab_comp WHERE hasToken(_content, 'rare5')) AS comp_rare5,
    (SELECT count() FROM tab_nocomp WHERE hasToken(_content, 'rare5')) AS nocomp_rare5,
    (comp_rare5 = 5) AS ok_rare5,
    (comp_rare5 = nocomp_rare5) AS ok_rare5_eq;

DROP TABLE tab_comp;

DROP TABLE tab_nocomp;
