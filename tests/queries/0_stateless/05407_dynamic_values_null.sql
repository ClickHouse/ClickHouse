-- Regression for #124877: Dynamic NULL parsing with both null_as_default settings.
SET enable_dynamic_type = 1;
SET input_format_null_as_default = 1;
SET input_format_values_interpret_expressions = 1;
SET input_format_values_accurate_types_of_literals = 1;

DROP TABLE IF EXISTS t_dynamic_values_null;
CREATE TABLE t_dynamic_values_null (id UInt8, d Dynamic) ENGINE = Memory;

DROP TABLE IF EXISTS t_values_null_compat;
CREATE TABLE t_values_null_compat
(
    id UInt8,
    n UInt32,
    s String,
    nullable_n Nullable(UInt32)
)
ENGINE = Memory;

-- Expression templates enabled.
SET input_format_values_deduce_templates_of_expressions = 1;
SELECT 'templates=1';

-- Keep the original six-row reproducer first. Then cover consecutive NULLs,
-- quoted 'NULL', a real zero, and returning to expression parsing.
INSERT INTO t_dynamic_values_null VALUES
    (1, toDate('2020-01-01')),
    (2, NULL),
    (3, 1 + 1),
    (4, NULL),
    (5, 42),
    (6, NULL),
    (7, NULL),
    (8, toDate('2020-01-02')),
    (9, 'NULL'),
    (10, NULL),
    (11, 1 + 1),
    (12, 0),
    (13, NULL),
    (14, 2 + 2),
    (15, NULL);

-- Check the NULL discriminator without depending on the integer width
-- chosen for non-NULL arithmetic expressions by the two parsing modes.
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

SELECT dynamicType(d) FROM t_dynamic_values_null WHERE id = 9;

TRUNCATE TABLE t_dynamic_values_null;

-- Baseline: NULL as the first value and after a plain literal.
INSERT INTO t_dynamic_values_null VALUES (1, NULL), (2, 42), (3, NULL);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

TRUNCATE TABLE t_dynamic_values_null;

-- The shared parsing path must retain NULL-as-default behavior for ordinary
-- columns and preserve actual NULLs in Nullable columns.
INSERT INTO t_values_null_compat VALUES
    (1, toUInt32(1) + 1, concat('a', 'b'), toUInt32(1) + 1),
    (2, NULL, NULL, NULL),
    (3, 3, 'NULL', 3),
    (4, NULL, NULL, NULL);

SELECT id, n, s, nullable_n, isNull(nullable_n)
FROM t_values_null_compat
ORDER BY id;

TRUNCATE TABLE t_values_null_compat;

-- Expression templates disabled.
SET input_format_values_deduce_templates_of_expressions = 0;
SELECT 'templates=0';

-- Keep the original six-row reproducer first. Then cover consecutive NULLs,
-- quoted 'NULL', a real zero, and returning to expression parsing.
INSERT INTO t_dynamic_values_null VALUES
    (1, toDate('2020-01-01')),
    (2, NULL),
    (3, 1 + 1),
    (4, NULL),
    (5, 42),
    (6, NULL),
    (7, NULL),
    (8, toDate('2020-01-02')),
    (9, 'NULL'),
    (10, NULL),
    (11, 1 + 1),
    (12, 0),
    (13, NULL),
    (14, 2 + 2),
    (15, NULL);

-- Check the NULL discriminator without depending on the integer width
-- chosen for non-NULL arithmetic expressions by the two parsing modes.
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

SELECT dynamicType(d) FROM t_dynamic_values_null WHERE id = 9;

TRUNCATE TABLE t_dynamic_values_null;

-- Baseline: NULL as the first value and after a plain literal.
INSERT INTO t_dynamic_values_null VALUES (1, NULL), (2, 42), (3, NULL);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

TRUNCATE TABLE t_dynamic_values_null;

-- The shared parsing path must retain NULL-as-default behavior for ordinary
-- columns and preserve actual NULLs in Nullable columns.
INSERT INTO t_values_null_compat VALUES
    (1, toUInt32(1) + 1, concat('a', 'b'), toUInt32(1) + 1),
    (2, NULL, NULL, NULL),
    (3, 3, 'NULL', 3),
    (4, NULL, NULL, NULL);

SELECT id, n, s, nullable_n, isNull(nullable_n)
FROM t_values_null_compat
ORDER BY id;

TRUNCATE TABLE t_values_null_compat;

-- Dynamic can hold NULL even when NULL-as-default conversion is disabled.
SET input_format_null_as_default = 0;

SET input_format_values_deduce_templates_of_expressions = 1;
SELECT 'null_as_default=0,templates=1';

-- Keep the original six-row reproducer first. Then cover consecutive NULLs,
-- quoted 'NULL', a real zero, and returning to expression parsing.
INSERT INTO t_dynamic_values_null VALUES
    (1, toDate('2020-01-01')),
    (2, NULL),
    (3, 1 + 1),
    (4, NULL),
    (5, 42),
    (6, NULL),
    (7, NULL),
    (8, toDate('2020-01-02')),
    (9, 'NULL'),
    (10, NULL),
    (11, 1 + 1),
    (12, 0),
    (13, NULL),
    (14, 2 + 2),
    (15, NULL);

-- Check the NULL discriminator without depending on the integer width
-- chosen for non-NULL arithmetic expressions by the two parsing modes.
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

SELECT dynamicType(d) FROM t_dynamic_values_null WHERE id = 9;

TRUNCATE TABLE t_dynamic_values_null;

-- Baseline: NULL as the first value and after a plain literal.
-- Literal-only parsing ensures this case exercises the deserializer directly.
SET input_format_values_interpret_expressions = 0;
INSERT INTO t_dynamic_values_null VALUES
    (1, NULL), (2, 42), (3, NULL), (4, null), (5, NuLl), (6, 'null'), (7, 'NuLl');
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

TRUNCATE TABLE t_dynamic_values_null;

SET input_format_values_interpret_expressions = 1;

SET input_format_values_deduce_templates_of_expressions = 0;
SELECT 'null_as_default=0,templates=0';

-- Keep the original six-row reproducer first. Then cover consecutive NULLs,
-- quoted 'NULL', a real zero, and returning to expression parsing.
INSERT INTO t_dynamic_values_null VALUES
    (1, toDate('2020-01-01')),
    (2, NULL),
    (3, 1 + 1),
    (4, NULL),
    (5, 42),
    (6, NULL),
    (7, NULL),
    (8, toDate('2020-01-02')),
    (9, 'NULL'),
    (10, NULL),
    (11, 1 + 1),
    (12, 0),
    (13, NULL),
    (14, 2 + 2),
    (15, NULL);

-- Check the NULL discriminator without depending on the integer width
-- chosen for non-NULL arithmetic expressions by the two parsing modes.
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

SELECT dynamicType(d) FROM t_dynamic_values_null WHERE id = 9;

TRUNCATE TABLE t_dynamic_values_null;

-- Baseline: NULL as the first value and after a plain literal.
-- Literal-only parsing ensures this case exercises the deserializer directly.
SET input_format_values_interpret_expressions = 0;
INSERT INTO t_dynamic_values_null VALUES
    (1, NULL), (2, 42), (3, NULL), (4, null), (5, NuLl), (6, 'null'), (7, 'NuLl');
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null
ORDER BY id;

TRUNCATE TABLE t_dynamic_values_null;

SET input_format_values_interpret_expressions = 1;

-- NULL-as-default must still propagate the missing-value marker after
-- expressions; with the setting disabled, an explicit NULL remains NULL.
SET input_format_defaults_for_omitted_fields = 1;
DROP TABLE IF EXISTS t_dynamic_values_null_default;
CREATE TABLE t_dynamic_values_null_default (id UInt8, d Dynamic DEFAULT 99) ENGINE = Memory;

SET input_format_null_as_default = 1;
SET input_format_values_deduce_templates_of_expressions = 1;
SELECT 'defaults:null_as_default=1,templates=1';
INSERT INTO t_dynamic_values_null_default VALUES
    (1, 1 + 1), (2, NULL), (3, 42), (4, NULL), (5, DEFAULT);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null_default
ORDER BY id;
TRUNCATE TABLE t_dynamic_values_null_default;

SET input_format_null_as_default = 1;
SET input_format_values_deduce_templates_of_expressions = 0;
SELECT 'defaults:null_as_default=1,templates=0';
INSERT INTO t_dynamic_values_null_default VALUES
    (1, 1 + 1), (2, NULL), (3, 42), (4, NULL), (5, DEFAULT);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null_default
ORDER BY id;
TRUNCATE TABLE t_dynamic_values_null_default;

SET input_format_null_as_default = 0;
SET input_format_values_deduce_templates_of_expressions = 1;
SELECT 'defaults:null_as_default=0,templates=1';
INSERT INTO t_dynamic_values_null_default VALUES
    (1, 1 + 1), (2, NULL), (3, 42), (4, NULL), (5, DEFAULT);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null_default
ORDER BY id;
TRUNCATE TABLE t_dynamic_values_null_default;

SET input_format_null_as_default = 0;
SET input_format_values_deduce_templates_of_expressions = 0;
SELECT 'defaults:null_as_default=0,templates=0';
INSERT INTO t_dynamic_values_null_default VALUES
    (1, 1 + 1), (2, NULL), (3, 42), (4, NULL), (5, DEFAULT);
SELECT id, d, isNull(d), dynamicType(d) = 'None' AS has_null_type
FROM t_dynamic_values_null_default
ORDER BY id;
TRUNCATE TABLE t_dynamic_values_null_default;

DROP TABLE t_dynamic_values_null_default;

DROP TABLE t_dynamic_values_null;
DROP TABLE t_values_null_compat;
