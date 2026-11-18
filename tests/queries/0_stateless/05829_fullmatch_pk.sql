-- Tests that fullMatch patterns are used for primary key pruning

DROP TABLE IF EXISTS mt_fullmatch_pk;
CREATE TABLE mt_fullmatch_pk (v String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO mt_fullmatch_pk VALUES ('a'), ('aaa'), ('aba'), ('bac'), ('acccca'), ('abc-xx'), ('abc-yy');

SET force_primary_key = 1;

-- The pattern is implicitly anchored, so its literal head is a key prefix
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'a.*');
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'ab.*');

-- A plain literal pattern is an equality
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'a');
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'aaa');

-- A non-perfect prefix is still usable for pruning
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'ab*a');

-- A top-level alternation of literals prunes by the common prefix of the branches
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'abc-xx|abc-yy');

-- An alternation without a common prefix cannot use the index
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, 'a|bac'); -- { serverError INDEX_NOT_USED }

-- A pattern without a fixed literal head cannot use the index
SELECT count() FROM mt_fullmatch_pk WHERE fullMatch(v, '.*a'); -- { serverError INDEX_NOT_USED }

DROP TABLE mt_fullmatch_pk;
