SELECT countSubstrings(materialize('aaa'), 'aa', 2), countSubstrings(materialize('aaa'), materialize('aa'), 2);
SELECT countSubstringsCaseInsensitive(materialize('aaaa'), 'AA', 3);
SELECT countSubstringsCaseInsensitiveUTF8(materialize('ȺȺȺ'), 'ⱥⱥ', 2);
SELECT s, n, countSubstrings(s, 'aa', n) FROM values('s String, n UInt8', ('a', 1), ('a', 2), ('aaa', 9), ('aaaa', 1), ('aaa', 2)) ORDER BY s, n;
