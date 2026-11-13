SET allow_experimental_shuffle_query = 1;
SET analyzer_inline_views = 0;
SET prefer_column_name_to_alias = 0, extremes = 0, exact_rows_before_limit = 0;

DROP VIEW IF EXISTS shuffle_view_05488;
CREATE VIEW shuffle_view_05488 SQL SECURITY INVOKER AS SELECT number FROM numbers(10) SHUFFLE;

-- The outer `ORDER BY ... LIMIT` must not be injected alongside the view's `SHUFFLE`.
SELECT number FROM shuffle_view_05488 ORDER BY number LIMIT 3;
SELECT number FROM shuffle_view_05488 ORDER BY number DESC LIMIT 3;

DROP VIEW shuffle_view_05488;
