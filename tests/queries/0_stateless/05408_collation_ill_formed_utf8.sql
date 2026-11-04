-- Tags: no-fasttest
-- no-fasttest: needs ICU

-- Order of valid text, embedded zero bytes and ill-formed UTF-8 under collations (an ill-formed sequence sorts as U+FFFD).
-- Strings that compare equal under a collation come out reversed with `id DESC`.

DROP TABLE IF EXISTS t_collation_utf8;
CREATE TABLE t_collation_utf8 (id UInt32, s String) ENGINE = Memory;
INSERT INTO t_collation_utf8 VALUES
    (1, 'a'), (2, 'A'), (3, 'b'), (4, 'B'), (5, 'z'), (6, 'á'), (7, 'a\xCC\x81'), (8, 'ä'),
    (9, 'å'), (10, 'ö'), (11, 'ß'), (12, 'ss'), (13, 'ı'), (14, 'i'), (15, 'İ'), (16, 'I'),
    (17, 'c'), (18, 'ch'), (19, 'cha'), (20, 'h'), (21, 'а'), (22, 'я'), (23, 'ё'), (24, '中'),
    (25, '日本'), (26, '😀'), (27, '𝔸'), (28, 'item2'), (29, 'item10'), (30, 'a-b'), (31, 'a b'), (32, 'ab'),
    (33, ''), (34, 'a\0b'), (35, 'a\0'), (36, '\xEF\xBF\xBD'), (37, 'a\xEF\xBF\xBDz'), (38, '\xEF\xBF\xBE'), (39, '\xEF\xBF\xBF'), (40, 'a\x80z'),
    (41, 'a\xC1\x81z'), (42, 'a\xE0\x82\x83z'), (43, 'a\xED\xA0\x80z'), (44, 'a\xED\xBF\xBFz'), (45, 'a\xF0\x8F\xBF\xBFz'), (46, 'a\xF4\x90\x80\x80z'), (47, 'a\xF5\x80z'), (48, 'a\xC3'),
    (49, 'a\xE2\x82'), (50, 'a\xE2\x82\xAC'), (51, 'a\xE2\x82b'), (52, '\xF0\x9F\x98'), (53, '\xFF'), (54, '\xC0\xAF'), (55, '\x80\x80'), (56, 'é'),
    (57, 'é\x80'), (58, 'ê'), (59, 'c\x80h');

SELECT 'en', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en', id);
SELECT 'en DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en', id DESC);
SELECT 'sv', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'sv', id);
SELECT 'sv DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'sv', id DESC);
SELECT 'tr', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'tr', id);
SELECT 'tr DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'tr', id DESC);
SELECT 'sk', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'sk', id);
SELECT 'sk DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'sk', id DESC);
SELECT 'ru', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'ru', id);
SELECT 'ru DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'ru', id DESC);
SELECT 'zh', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'zh', id);
SELECT 'zh DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'zh', id DESC);
SELECT 'ja', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'ja', id);
SELECT 'ja DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'ja', id DESC);
SELECT 'en-u-ks-level1', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ks-level1', id);
SELECT 'en-u-ks-level1 DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ks-level1', id DESC);
SELECT 'en-u-ks-identic', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ks-identic', id);
SELECT 'en-u-ks-identic DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ks-identic', id DESC);
SELECT 'en-u-kn-true', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-kn-true', id);
SELECT 'en-u-kn-true DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-kn-true', id DESC);
SELECT 'en-u-ka-shifted', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ka-shifted', id);
SELECT 'en-u-ka-shifted DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-ka-shifted', id DESC);
SELECT 'en-u-kf-upper', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-kf-upper', id);
SELECT 'en-u-kf-upper DESC', groupArray(id) FROM (SELECT id FROM t_collation_utf8 ORDER BY s COLLATE 'en-u-kf-upper', id DESC);

DROP TABLE t_collation_utf8;
