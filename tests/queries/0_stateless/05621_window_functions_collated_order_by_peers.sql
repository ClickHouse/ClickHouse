-- Tags: no-fasttest
-- no-fasttest: collations need ICU

-- Rows that are equal under the collation of the window ORDER BY are peers, even when their bytes differ.
-- The same letter is written below in two ways: as one code point, and as a base letter with a combining accent.

SELECT rank() OVER w, count() OVER w, cume_dist() OVER w
FROM values('x String', '\xC3\xA9', 'e\xCC\x81', '\xC3\xA9')
WINDOW w AS (ORDER BY x COLLATE 'en');
