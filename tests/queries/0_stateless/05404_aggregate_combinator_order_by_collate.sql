-- Tags: no-fasttest
-- no-fasttest: needs ICU

-- COLLATE in ORDER BY inside the arguments of an aggregate function is respected.
SELECT groupArray(s ORDER BY s COLLATE 'tr'), groupArray(s ORDER BY s) FROM values('s String', ('i'), ('ı'), ('h'), ('j'));
SELECT groupArrayOrderBy('0 ASC COLLATE \'tr\'')(s, s) FROM values('s String', ('i'), ('ı'), ('h'), ('j'));
