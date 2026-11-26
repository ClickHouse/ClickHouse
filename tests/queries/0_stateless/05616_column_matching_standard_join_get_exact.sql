-- `joinGet` keeps a double-quoted table name exact under `standard` matching also when the function
-- resolves the table again from its name, so case siblings `Jt_Exact` and `jt_exact` do not collide.

SET database_and_table_name_matching = 'standard';

CREATE TABLE "Jt_Exact" (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE "jt_exact" (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO "Jt_Exact" VALUES (1, 'upper');
INSERT INTO "jt_exact" VALUES (1, 'lower');

SELECT joinGet("Jt_Exact", 'v', toUInt64(1));
SELECT joinGet("jt_exact", 'v', toUInt64(1));
SELECT joinGetOrNull("Jt_Exact", 'v', toUInt64(2));
SELECT joinGet("Jt_Exact", 'v', number) FROM numbers(2) SETTINGS join_use_nulls = 1;

-- A folded reference matches both tables.
SELECT joinGet(Jt_Exact, 'v', toUInt64(1)); -- { serverError AMBIGUOUS_IDENTIFIER }

-- Under `sensitive` matching the name is exact anyway.
SELECT joinGet(Jt_Exact, 'v', toUInt64(1)) SETTINGS database_and_table_name_matching = 'sensitive';
SELECT joinGet('jt_exact', 'v', toUInt64(1)) SETTINGS database_and_table_name_matching = 'sensitive';

DROP TABLE "Jt_Exact";
DROP TABLE "jt_exact";
