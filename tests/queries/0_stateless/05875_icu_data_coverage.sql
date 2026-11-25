-- Tags: no-fasttest
-- no-fasttest: needs ICU

-- The ICU data in `contrib/icudata` is filtered to the parts that ClickHouse reads.
-- This test touches each of them, so that a part does not get lost when the data is regenerated:
-- collation, the English names of languages, converters, normalization, case mapping,
-- break iteration with dictionaries, character properties and names.

SELECT 'collations', count() > 100, countIf(language = '') FROM system.collations;
SELECT name, language FROM system.collations WHERE name IN ('ar', 'de', 'en_US', 'ja', 'ru', 'sr_Latn', 'tr', 'zh') ORDER BY name;

SELECT 'ru', groupArray(s) FROM (SELECT arrayJoin(['я', 'а', 'Б', 'ё', 'е', 'Ж', 'ж']) AS s ORDER BY s COLLATE 'ru');
SELECT 'tr', groupArray(s) FROM (SELECT arrayJoin(['i', 'I', 'ı', 'İ', 'z', 'ç', 'c']) AS s ORDER BY s COLLATE 'tr');
SELECT 'sv', groupArray(s) FROM (SELECT arrayJoin(['z', 'å', 'ä', 'ö', 'a']) AS s ORDER BY s COLLATE 'sv');
SELECT 'zh', groupArray(s) FROM (SELECT arrayJoin(['中', '文', '汉', '字', '阿']) AS s ORDER BY s COLLATE 'zh');
SELECT 'ja', groupArray(s) FROM (SELECT arrayJoin(['カ', 'か', 'あ', 'ア', '漢', '亜']) AS s ORDER BY s COLLATE 'ja');
SELECT 'ko', groupArray(s) FROM (SELECT arrayJoin(['하', '가', '다', '나']) AS s ORDER BY s COLLATE 'ko');
SELECT 'de phonebook', groupArray(s) FROM (SELECT arrayJoin(['Müller', 'Mueller', 'Muller', 'Mz']) AS s ORDER BY s COLLATE 'de@collation=phonebook');
SELECT 'es traditional', groupArray(s) FROM (SELECT arrayJoin(['chico', 'cuna', 'dedo', 'llama', 'luna']) AS s ORDER BY s COLLATE 'es@collation=traditional');

SELECT 'CP1251', hex(convertCharset('Привет', 'UTF-8', 'CP1251')), convertCharset(unhex('CFF0E8E2E5F2'), 'CP1251', 'UTF-8');
SELECT 'Shift_JIS', hex(convertCharset('日本語', 'UTF-8', 'Shift_JIS'));
SELECT 'GB18030', hex(convertCharset('中文', 'UTF-8', 'GB18030'));
SELECT 'EUC-KR', hex(convertCharset('한국', 'UTF-8', 'EUC-KR'));
SELECT 'KOI8-R', hex(convertCharset('мир', 'UTF-8', 'KOI8-R'));
SELECT 'ISO-8859-15', hex(convertCharset('€', 'UTF-8', 'ISO-8859-15'));
SELECT 'UTF-16LE', hex(convertCharset('aé', 'UTF-8', 'UTF-16LE'));
SELECT 'ibm-037', hex(convertCharset('abc', 'UTF-8', 'ibm-037'));

SELECT 'normalize', hex(normalizeUTF8NFC('é')), hex(normalizeUTF8NFD('é')), normalizeUTF8NFKC('ﬁ①'), hex(normalizeUTF8NFKD('Å'));
SELECT 'case', lowerUTF8('ÀÉÎÕÜ ПРИВЕТ ΣΑΣ'), upperUTF8('straße привет ǆ');

SELECT 'tokens en', tokens('Hello, world! Привет мир.', 'icu(''en'')');
SELECT 'tokens zh', tokens('我爱北京天安门', 'icu(''zh'')');
SELECT 'tokens ja', tokens('こんにちは世界', 'icu(''ja'')');
SELECT 'tokens th', tokens('สวัสดีชาวโลก', 'icu(''th'')');
SELECT 'tokens km my lo', tokens('ភាសាខ្មែរ မြန်မာဘာသာ ພາສາລາວ', 'icu(''en'')');

SELECT 'unicode', count() > 100000 FROM system.unicode;
SELECT code_point, name FROM system.unicode WHERE code_point IN ('A', 'я', '中', '€') ORDER BY code_point;
