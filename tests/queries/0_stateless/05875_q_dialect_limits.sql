-- The limits on the depth of the tree and on the expansion of a program of the `q` dialect.

SET enable_q_dialect = 1;
SET dialect = 'q';

SET max_parser_depth = 20;
/ A long expression without parentheses is as deep as it is long, so `max_parser_depth` limits it like the nesting
/ of the brackets: a sequence of operators, a sequence of brackets after an item, and the body of a lambda.
1+1+1+1+1+1+1+1+1+1;
1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1; -- { clientError TOO_DEEP_RECURSION }
til[5][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0]; -- { clientError TOO_DEEP_RECURSION }
{x+x+x} 1;
{x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x} 1; -- { clientError TOO_DEEP_RECURSION }
SET max_parser_depth = 1000;
1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1+1;

/ The values of a program share their parts: `a:a+a` refers to the previous `a` twice, so thirty of them stand for
/ an expression of a billion items. The expansion is limited by `max_expanded_ast_elements`.
a:1; a:a+a; a:a+a; a:a+a; a;
a:1; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a; -- { serverError TOO_BIG_AST }

SET max_expanded_ast_elements = 5000;
a:1; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a;
a:1; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a:a+a; a; -- { serverError TOO_BIG_AST }

/ A function is inlined with its argument wherever it mentions the parameter.
f:{x+x}; f f f 1;
f:{x+x}; f f f f f f f f f f f f 1; -- { serverError TOO_BIG_AST }

/ A table is copied into every query that reads it.
t:([] a:1 2); t:t,t; t:t,t; count t;
t:([] a:1 2); t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; t:t,t; count t; -- { serverError TOO_BIG_AST }
