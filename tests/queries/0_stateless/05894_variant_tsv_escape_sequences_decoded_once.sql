-- A value with escape sequences read into a Variant with the Escaped rule must decode them once, as a column of the variant's type does.

-- TSV bytes, one per row: C:\\temp | a\tb | a\nb | \\N | abc\r
SELECT hex(s) FROM format(TSV, 's String', 'C:\\\\temp\na\\tb\na\\nb\n\\\\N\nabc\\r');
SELECT hex(toString(v)), variantType(v) FROM format(TSV, 'v Variant(Int64, String)', 'C:\\\\temp\na\\tb\na\\nb\n\\\\N\nabc\\r');

-- Unchanged: NULL, a number, a plain string.
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(Int64, String)', '\\N\n42\nhello');

-- A non-String variant: TSV bytes ['a\\b'] (what the TSV writer emits for ['a\b']).
SELECT hex(arrayJoin(a)) FROM format(TSV, 'a Array(String)', '[''a\\\\b'']');
SELECT hex(arrayJoin(variantElement(v, 'Array(String)'))), variantType(v) FROM format(TSV, 'v Variant(Array(String), Int64)', '[''a\\\\b'']');

-- CRLF line endings.
SELECT hex(toString(v)), variantType(v) FROM format(TSV, 'v Variant(Int64, String)', 'C:\\\\temp\r\n42\r\n') SETTINGS input_format_tsv_crlf_end_of_line = 1;

-- Another format with the Escaped rule.
SELECT hex(toString(v)) FROM format(CustomSeparated, 'v Variant(Int64, String)', 'C:\\\\temp\n');

-- A query parameter.
SET param_p = 'C:\\\\temp';
SELECT hex({p:String}), hex(toString({p:Variant(Int64, String)}));

-- Schema inference that infers a Variant (inference settings pinned against randomization).
SELECT hex(toString(c1)), variantType(c1) FROM format(TSV, '1\na\\\\tb') SETTINGS input_format_try_infer_variants = 1, input_format_try_infer_integers = 1;
