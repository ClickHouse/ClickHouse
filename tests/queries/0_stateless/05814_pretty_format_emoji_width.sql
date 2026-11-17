-- Every value takes four terminal columns, so the borders line up: emoji take two columns, including the newer ones.
SELECT * FROM values('s String, kind String', ('abcd', 'ASCII'), ('一二', 'CJK'), ('😀😀', 'emoji'), ('⌚⌚', 'watch'), ('🥲🫠', 'Unicode 13 and 14 emoji')) FORMAT PrettyCompactMonoBlock;

-- A value cut before an emoji that does not fit is padded to the same border as the others.
SELECT * FROM values('s String', ('😀😀😀'), ('abcdef')) FORMAT PrettyCompactMonoBlock SETTINGS output_format_pretty_max_value_width = 3;

-- A long column name of emoji is cut to the width of the column.
SELECT 1 AS `😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀` FORMAT PrettyCompactMonoBlock;
SELECT 1 AS `😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀` FORMAT Vertical;
