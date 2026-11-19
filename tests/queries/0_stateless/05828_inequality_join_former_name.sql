-- The `inequality` value of `join_algorithm` was named `ie_join` before 26.11. The former name is still accepted
-- and is shown as `inequality`.

SET enable_analyzer = 1;

SET join_algorithm = 'ie_join';
SELECT getSetting('join_algorithm');

SET join_algorithm = 'ie_join,hash';
SELECT getSetting('join_algorithm');

SET join_algorithm = 'ie_join';
SELECT count() FROM numbers(10) AS l JOIN numbers(10) AS r ON l.number < r.number AND l.number + 3 > r.number;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM numbers(10) AS l JOIN numbers(10) AS r ON l.number < r.number AND l.number + 3 > r.number) WHERE explain LIKE '%InequalityJoin%';
