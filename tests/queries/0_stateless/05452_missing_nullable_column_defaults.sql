DROP TABLE IF EXISTS missing_nullable_defaults;
CREATE TABLE missing_nullable_defaults (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO missing_nullable_defaults VALUES (1), (2);

ALTER TABLE missing_nullable_defaults
    ADD COLUMN text Nullable(String),
    ADD COLUMN text_type String DEFAULT toTypeName(assumeNotNull(text)),
    ADD COLUMN number Nullable(UInt32),
    ADD COLUMN number_type String DEFAULT toTypeName(assumeNotNull(number)),
    ADD COLUMN lc_text LowCardinality(Nullable(String)),
    ADD COLUMN lc_type String DEFAULT toTypeName(assumeNotNull(lc_text)),
    ADD COLUMN json_value String DEFAULT JSONExtract(assumeNotNull(text), 'class', 'String');

SELECT id, text_type, number_type, lc_type FROM missing_nullable_defaults ORDER BY id;
SELECT id, json_value = JSONExtract(assumeNotNull(CAST(NULL AS Nullable(String))), 'class', 'String') AS matches_typed_null
FROM missing_nullable_defaults ORDER BY id;

INSERT INTO missing_nullable_defaults (id, text, number, lc_text) VALUES
    (3, '{"class":"Mage"}', 7, 'stored'),
    (4, NULL, NULL, NULL);

SELECT id, text_type, number_type, lc_type FROM missing_nullable_defaults WHERE id >= 3 ORDER BY id;
SELECT id, json_value FROM missing_nullable_defaults WHERE id = 3;
SELECT id, json_value = JSONExtract(assumeNotNull(CAST(NULL AS Nullable(String))), 'class', 'String') AS matches_typed_null
FROM missing_nullable_defaults WHERE id = 4;

DROP TABLE missing_nullable_defaults;