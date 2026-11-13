SET allow_lossy_numeric_supertype = 1;
SET use_variant_as_common_type = 0;

-- Projection definitions are analyzed with the server profile, so verify the
-- setting's type-resolution role directly with a session-scoped expression.
SELECT sum(if(n = 0, d, x))
FROM values('n UInt8, d Decimal(18, 2), x Float64', (0, 1.25, 2.5));

DROP TABLE IF EXISTS squality_numeric_expression_index;
CREATE TABLE squality_numeric_expression_index
(
    d Decimal(18, 0),
    x Float64,
    INDEX i (d = x) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_numeric_expression_index
VALUES ('9007199254740993', 9007199254740992.);

SELECT count() FROM squality_numeric_expression_index WHERE (d = x) = 0
SETTINGS use_skip_indexes = 0;
SELECT count() FROM squality_numeric_expression_index WHERE (d = x) = 0
SETTINGS use_skip_indexes = 1;

DROP TABLE squality_numeric_expression_index;
