-- Nested collections in dictionary arguments, in query parameter values, and the ones the expression parser folds into one literal count towards `max_parser_depth`.
SELECT formatQuery(concat('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(HOST ', repeat('[', 500), '1', repeat(']', 500), ')) LAYOUT(FLAT()) LIFETIME(0)')) SETTINGS max_parser_depth = 300; -- { serverError TOO_DEEP_RECURSION }
SELECT formatQuery(concat('SET param_x = ', repeat('[', 500), '1', repeat(']', 500))) SETTINGS max_parser_depth = 300; -- { serverError TOO_DEEP_RECURSION }
SELECT formatQuery(concat('SELECT ', repeat('[(', 500), '1', repeat(')]', 500))) SETTINGS max_parser_depth = 300; -- { serverError TOO_DEEP_RECURSION }
SELECT formatQuery(concat('SELECT ', repeat('((1), ', 500), '1', repeat(')', 500))) SETTINGS max_parser_depth = 300; -- { serverError TOO_DEEP_RECURSION }
SELECT formatQuery(concat('CREATE DICTIONARY d (a UInt64) PRIMARY KEY a SOURCE(CLICKHOUSE(HOST ', repeat('[', 500), '1', repeat(']', 500), ')) LAYOUT(FLAT()) LIFETIME(0)')) != '' SETTINGS max_parser_depth = 1000;
