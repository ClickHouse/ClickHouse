-- Column matchers in an INSERT column list: `(COLUMNS(a, b))`, `(COLUMNS('re'))` and the
-- qualified `(t.COLUMNS('re'))`, with the EXCEPT modifier. Behaviour source:
-- docs/reference/statements/insert-into.mdx ("You can also use an expression with column
-- matcher ... and/or modifiers such as APPLY, EXCEPT, REPLACE") and the COLUMNS expression in
-- docs/reference/statements/select/index.mdx.
-- The matcher is rewritten into an explicit column list before the insert runs, so which column
-- each value lands in -- and therefore the stored row -- is the user-visible result.
-- Memory engine: no merge-tree setting takes part, nothing to pin.

DROP TABLE IF EXISTS tqn;
CREATE TABLE tqn (id Int32, a Int32, b Int32, c Int32) ENGINE = Memory;

-- A COLUMNS(<list>) matcher expands to exactly the named columns, in the order they are written,
-- so (b, a) takes the two values the other way round. Unnamed columns keep their default.
INSERT INTO tqn (id, COLUMNS(a, b)) VALUES (1, 11, 12);
INSERT INTO tqn (id, COLUMNS(b, a)) VALUES (2, 22, 21);
-- EXCEPT is applied to the expansion of the list matcher.
INSERT INTO tqn (id, COLUMNS(a, b, c) EXCEPT c) VALUES (3, 31, 32);

-- An unqualified COLUMNS('re') matcher expands in the order of the table's columns.
INSERT INTO tqn (id, COLUMNS('^[ab]$')) VALUES (4, 41, 42);

-- A qualified matcher resolves its qualifier against the insertion table and then expands the
-- same way: in the order of the table's columns, not in the order of the pattern's alternatives.
INSERT INTO tqn (id, tqn.COLUMNS('^[ab]$')) VALUES (5, 51, 52);
INSERT INTO tqn (id, tqn.COLUMNS('^[cb]$')) VALUES (6, 62, 63);
-- EXCEPT is applied to the expansion of the qualified matcher too.
INSERT INTO tqn (id, tqn.COLUMNS('^[abc]$') EXCEPT (b, c)) VALUES (7, 71);

SELECT id, a, b, c FROM tqn ORDER BY id;

-- A pattern that re2 cannot compile is rejected, qualified or not.
INSERT INTO tqn (id, COLUMNS('(')) VALUES (8, 81); -- { serverError CANNOT_COMPILE_REGEXP }
INSERT INTO tqn (id, tqn.COLUMNS('(')) VALUES (8, 81); -- { serverError CANNOT_COMPILE_REGEXP }
-- The qualifier of a qualified matcher is resolved before the pattern is ever used, so an
-- unknown qualifier is rejected even when the pattern itself is fine.
INSERT INTO tqn (id, nosuch.COLUMNS('^[ab]$')) VALUES (8, 81, 82); -- { serverError UNKNOWN_IDENTIFIER }
INSERT INTO tqn (id, nosuch.*) VALUES (8, 81, 82); -- { serverError UNKNOWN_IDENTIFIER }

-- None of the rejected inserts stored a row.
SELECT count() FROM tqn;

DROP TABLE tqn;
