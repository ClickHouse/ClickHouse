-- Every value takes four terminal columns, so the borders line up: emoji take two columns, including the newer ones.
SELECT * FROM values('s String, kind String', ('abcd', 'ASCII'), ('一二', 'CJK'), ('😀😀', 'emoji'), ('⌚⌚', 'watch'), ('🥲🫠', 'Unicode 13 and 14 emoji')) FORMAT PrettyCompactMonoBlock;
