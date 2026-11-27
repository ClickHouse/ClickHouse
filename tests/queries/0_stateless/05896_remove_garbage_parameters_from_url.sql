-- The example from the issue: only `cst` goes.
SELECT removeGarbageParametersFromURL('http://yandex.ru/clck/jsredir?from=yandex.ru;search%2F;web;;&text=&etext=1004&cst=AiuY0DBWFJ5Hyx_fyvalFPA3abBqdnSOApSiLPWwVkIeiz46AroRCQXrfJ8M5oYTWorWEWccK4Kw_QEhSDD6X4nGMT4OabEk0xnry4NtnOEzWFPU4iTzunSVVjkuY7CYolnD7hb04cMRv7iMnaO8LjNm0hxqvwN9sXCzUYeXp_muLsdY4W99_U5MJKGmz7IAmR5-ceoAoaBB2XGYAS9BTYKbbvlmneBpbf_SwAd_6OOACXtLmRXqXad3AQbcArYE8LCO0zmE9vpha3yoT0jl8pd9CUmbGZR5nA3sf5TcDFTpr5nYaOdxjmHep2cZeW3QHvPtKA2xWXW6qzGrQeZ1SEOPcJ1afJqmAHisup90hhNYyl2hxl8xn_DtCRbJqYHb88JtuQ3591EGW42wPZhSbxBFdU0KIZN3c_VZOmk6avzKzqG_kJpjPObWXbh9qs0S23WxDGCcPUrIzi3ESSLv1qgaRhqkfjBc57BFVA4RxlljpKQdeVeTbklJgqptznf1aHZQ2wYARBzC_jvv994MCTZIus_NctCMWoSaU74OaMmo0h5ScYLI2CWy6nj5PbhCrgeLsaEBVOQT9xoLSoCRfJ78xI_T1ruuD3QBJmHY6YW8f5UM36LRbzhd5vmNTPRvrs2wcCFhF_w&l10n=ru&cts=1458904227333&mc=1.584962500721156');

-- Random tokens: base64url without padding (11 characters), standard base64 with padding, JWT, hex in both cases.
-- The 15-digit hex value is too short.
SELECT removeGarbageParametersFromURL('https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42');
SELECT removeGarbageParametersFromURL('https://example.com/?a=1&sig=q3Cx9+Lm/2RtWv8kZp4N7A==&b=2');
SELECT removeGarbageParametersFromURL('https://example.com/callback#access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c&token_type=bearer');
SELECT removeGarbageParametersFromURL('https://example.com/?h=d41d8cd98f00b204e9800998ecf8427e&H=D41D8CD98F00B204E9800998ECF8427E&s=d41d8cd98f00b20');

-- Long values that are not close to uniform stay.
SELECT removeGarbageParametersFromURL('https://example.com/?a=aaaaaaaaaaaaaaaa&b=0000000000000000&c=1458904227333123&d=0a0a0a0a0a0a0a0a&e=deadbeefdeadbeef');
SELECT removeGarbageParametersFromURL('https://example.com/?utm_campaign=SummerSaleCampaign&utm_content=summer_sale_2024&q=how+to+cook+pasta+at+home&host=www.example.com&path=/account/settings/profile&ts=1458904227333&v=1.584962500721156');

-- Position of the removed parameter, flags, empty values, hash routes and `?` inside a name or a value.
SELECT removeGarbageParametersFromURL(arrayJoin([
    'https://example.com/?v=dQw4w9WgXcQ&a=1',
    'https://example.com/?a=1&v=dQw4w9WgXcQ',
    'https://example.com/?v=dQw4w9WgXcQ',
    'https://example.com/?v=dQw4w9WgXcQ#top',
    'https://example.com/?a=1#v=dQw4w9WgXcQ',
    'https://example.com/?flag&v=dQw4w9WgXcQ&&e=&dQw4w9WgXcQ',
    'https://example.com/#/route?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c&tab=1',
    'https://example.com/#/route?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c',
    'https://example.com/?a=1&x?v=dQw4w9WgXcQ&b=2',
    'https://example.com/?redirect=/a?v=dQw4w9WgXcQ&b=2',
    'https://example.com/path/dQw4w9WgXcQ',
    '',
    '?'
]));

-- Column types.
SELECT removeGarbageParametersFromURL(materialize('https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42'));
SELECT removeGarbageParametersFromURL(arrayJoin([toNullable('https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42'), NULL])) AS url, toTypeName(url);
SELECT removeGarbageParametersFromURL(toLowCardinality('https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42')) AS url, toTypeName(url);
SELECT removeGarbageParametersFromURL(toFixedString('https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42', 48)); -- { serverError ILLEGAL_COLUMN }
