-- Random settings limits: optimize_rewrite_regexp_functions=(1, None)
-- A quantified leading `^` is optional, so the pattern can match at any offset and `replaceRegexpAll` must stay a global replace.

SELECT replaceRegexpAll(materialize('xfx'), '^?x', 'Z'), replaceRegexpAll(materialize('xfx'), '^*x', 'Z'), replaceRegexpAll(materialize('xfx'), '^{0,3}x', 'Z'), replaceRegexpAll(materialize('xfx'), '^??x', 'Z'), regexp_replace(materialize('xfx'), '^?x', 'Z');
SELECT replaceRegexpAll(materialize('xfx'), '^\\Q\\E?x', 'Z'), replaceRegexpAll(materialize('xfx'), '^(?i)?x', 'Z');
SELECT replaceRegexpAll(materialize('xfx'), '^?x', 'Z'), replaceRegexpAll(materialize('xfx'), '^*x', 'Z'), replaceRegexpAll(materialize('xfx'), '^{0,3}x', 'Z'), replaceRegexpAll(materialize('xfx'), '^??x', 'Z'), regexp_replace(materialize('xfx'), '^?x', 'Z') SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(materialize('xfx'), '^\\Q\\E?x', 'Z'), replaceRegexpAll(materialize('xfx'), '^(?i)?x', 'Z') SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('xfx'), '^?x', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('xfx'), '^x', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('http://www.a.com/x'), '^https?://(?:www\\.)?([^/]+)/.*$', '\\1')) WHERE explain LIKE '%replaceRegexpOne%';
