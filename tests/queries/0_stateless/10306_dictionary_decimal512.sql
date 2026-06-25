DROP DICTIONARY IF EXISTS dict_decimal512_stage7;

CREATE DICTIONARY dict_decimal512_stage7
(
    key   UInt64,
    value Decimal512(3) DEFAULT '1.500'   -- 纯字面量，允许
)
PRIMARY KEY key
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

SELECT toTypeName(dictGet('dict_decimal512_stage7', 'value', toUInt64(42))) AS type_name,
       dictGet('dict_decimal512_stage7', 'value', toUInt64(42))             AS dict_value;

DROP DICTIONARY dict_decimal512_stage7;