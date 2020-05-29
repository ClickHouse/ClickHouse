parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

query_list: query_stmt (SEMICOLON query_stmt)* SEMICOLON?;

query_stmt: select_union_stmt;

// SELECT statement

select_union_stmt:
    select_stmt (UNION ALL select_stmt)*
    (INTO OUTFILE STRING_LITERAL)?
    (FORMAT identifier)?
    ;
select_stmt:
    (WITH column_expr_list)?
    SELECT DISTINCT? column_expr_list
    (FROM join_expr FINAL?)?
    (SAMPLE ratio_expr (OFFSET ratio_expr)?)?
    (LEFT? ARRAY JOIN column_expr_list)?
    (PREWHERE column_expr)?
    (WHERE column_expr)?
    (GROUP BY column_expr_list (WITH TOTALS)?)?
    (HAVING column_expr)?
    (ORDER BY order_expr_list)?
    (LIMIT limit_expr BY column_expr_list)?
    (LIMIT limit_expr)?
    (SETTINGS setting_expr_list)?
    ;

join_expr: table_identifier; // TODO: not complete!
limit_expr: NUMBER_LITERAL (COMMA NUMBER_LITERAL)? | NUMBER_LITERAL OFFSET NUMBER_LITERAL;
order_expr_list: order_expr (COMMA order_expr)*;
order_expr: column_expr (ASCENDING | DESCENDING)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratio_expr: NUMBER_LITERAL (SLASH NUMBER_LITERAL); // TODO: not complete!
setting_expr_list: setting_expr (COMMA setting_expr)*;
setting_expr: identifier EQ LITERAL;

// Columns

column_expr_list: column_expr (COMMA column_expr)*;
column_expr:
    ( LPAREN column_expr RPAREN                 // tuple
    | LPAREN select_stmt RPAREN                 // subquery
    | LBRACKET column_expr_list? RBRACKET       // array
    | column_expr LBRACKET column_expr RBRACKET // array access
    | column_expr DOT NUMBER_LITERAL            // tuple access
    | column_op_expr                            // operator
    | column_function_expr                      // function call
    | column_identifier                         // identifier
    | LITERAL                                   // literal
    | ASTERISK                                  // * (all columns)
    )
    (AS identifier)?                            // alias
    ;
column_op_expr
    : unary_op column_expr
    | column_expr IS NOT? NULL
    | column_expr binary_op column_expr
    | column_expr QUERY column_expr COLON column_expr
    | column_expr NOT? BETWEEN column_expr AND column_expr
    | CASE column_expr? (WHEN column_expr THEN column_expr)+ (ELSE column_expr)? END
    | INTERVAL column_expr INTERVAL_TYPE
    ;
column_function_expr
    : identifier (LPAREN (LITERAL (COMMA LITERAL)*)? RPAREN)? LPAREN column_arg_list? RPAREN
    | EXTRACT LPAREN INTERVAL_TYPE FROM column_expr RPAREN
    | CAST LPAREN column_expr AS identifier RPAREN
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM column_expr RPAREN
    ;
column_arg_list: column_arg_expr (COMMA column_arg_expr)*;
column_arg_expr: column_expr | column_lambda_expr;
column_lambda_expr:
    ( LPAREN identifier (COMMA identifier)* RPAREN
    |        identifier (COMMA identifier)*
    )
    ARROW column_expr
    ;
column_identifier: (table_identifier DOT)? identifier; // TODO: don't forget compound identifier.

// Tables

table_identifier: (database_identifier DOT)? identifier;

// Databases

database_identifier: identifier;

// Basics

identifier: IDENTIFIER; // TODO: not complete!
unary_op: DASH | NOT;
binary_op
    : ASTERISK | SLASH | PERCENT | PLUS | DASH | EQ | NOT_EQ | LE | GE | LT | GT | CONCAT // signs
    | AND | OR | NOT? LIKE | GLOBAL? NOT? IN                                              // keywords
    ;
