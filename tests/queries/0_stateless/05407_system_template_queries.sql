-- Tags: no-parallel

DELETE FROM system.template_queries WHERE name = '05407_q1';

SELECT count() > 0 FROM system.template_queries WHERE is_builtin;
SELECT count() FROM system.template_queries WHERE NOT is_builtin AND name LIKE '05407_%';

INSERT INTO system.template_queries (name, category, description, query) VALUES ('05407_q1', 'test', 'Test\tquery', 'SELECT 1\nFROM system.one');
SELECT name, category, description, query, is_builtin, created_at > 0 FROM system.template_queries WHERE name = '05407_q1';

-- Duplicate user name
INSERT INTO system.template_queries (name, category, description, query) VALUES ('05407_q1', 'test', 'dup', 'SELECT 2'); -- { serverError TABLE_ALREADY_EXISTS }

-- Collision with a built-in name
INSERT INTO system.template_queries (name, category, description, query)
SELECT name, 'test', 'dup', 'SELECT 2' FROM system.template_queries WHERE is_builtin LIMIT 1; -- { serverError ACCESS_DENIED }

-- Deleting a built-in is forbidden.
DELETE FROM system.template_queries WHERE name = 'Biggest tables'; -- { serverError ACCESS_DENIED }

-- Only equality on name is supported
DELETE FROM system.template_queries WHERE category = 'test'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.template_queries WHERE name = '05407_q1';
DELETE FROM system.template_queries WHERE name = '05407_q1';
SELECT count() FROM system.template_queries WHERE name = '05407_q1';
