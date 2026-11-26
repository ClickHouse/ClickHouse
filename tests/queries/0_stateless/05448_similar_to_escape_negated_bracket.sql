-- https://github.com/ClickHouse/ClickHouse/issues/123831
-- A negated bracket `[^...]` must mean the same with a custom ESCAPE as without it.

SELECT s, s SIMILAR TO '[^a]' ESCAPE '#', s SIMILAR TO '[^a]' FROM (SELECT arrayJoin(['^', 'a', 'b']) AS s) ORDER BY s;
SELECT 'b' SIMILAR TO '[^-z]' ESCAPE '#', '-' SIMILAR TO '[^-z]' ESCAPE '#', '^' NOT SIMILAR TO '[^a]' ESCAPE '#';
SELECT similarTo('^', materialize('[^a]'), '#');
SELECT 'a' SIMILAR TO '[^-0-9]' ESCAPE '#';
