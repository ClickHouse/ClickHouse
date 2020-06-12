parser grammar ClickHouseParser;

options {
	tokenVocab=ClickHouseLexer;
}

// эта грамматика написана по сорсам парсеров, имена правил примерно соответствуют парсерам в cpp.
// известные расхождения
// 1. скобки не обязательно сразу идут после имени функции.
// 2. многословные токены поделены на самостоятельные слова
// 3. для INSERT запроса не написана часть парсинга значений.
// 4. правило для expr переписано чтобы понизить глубину AST и сразу выходить на уровень expr - al

parse
 : ( query | err ) EOF
 ;

query
 :    show_tables_query
 |    select_query
 |    insert_query
 |    create_query
 |    rename_query
 |    drop_query
 |    alter_query
 |    use_query
 |    set_query
 |    optimize_query
 |    table_properties_query
 |    show_processlist_query
 |    check_query
 |    kill_query_query
 ;

// 1. QUERIES

select_query
 :  select_query_main ( K_UNION K_ALL select_query_main ) *
    query_outfile_step?
    select_format_step?
 ;

select_query_main
 :  select_with_step?
    select_select_step select_from_step?
    K_FINAL? select_sample_step?
    select_array_join_step? select_join_step?
    select_prewhere_step? select_where_step?
    select_groupby_step? select_having_step?
    select_orderby_step?
    select_limitby_step? select_limit_step?
    select_settings_step?
 ;

select_with_step
 : K_WITH select_expr_list
 ;

select_select_step
 : K_SELECT K_DISTINCT? select_expr_list
 ;

select_from_step
 : K_FROM ( full_table_name
          | table_function
          | subquery
          ) select_alias?
 ;

select_array_join_step
 : K_LEFT? K_ARRAY K_JOIN not_empty_expression_list
 ;

select_sample_step
 : K_SAMPLE sample_ratio (K_OFFSET sample_ratio ) ?
 ;

sample_ratio
 : NUMERIC_LITERAL ( DIVIDE NUMERIC_LITERAL ) ?
 ;

select_join_step
 :  K_GLOBAL?
        ( K_ANY | K_ALL ) ( K_INNER | K_LEFT K_OUTER? | K_RIGHT K_OUTER? | K_FULL K_OUTER? ) K_JOIN select_join_right_part
      ( K_USING LPAREN not_empty_expression_list RPAREN
      | K_USING not_empty_expression_list
      // | K_ON expr  на самом деле нет.
      )
 |  K_GLOBAL? K_CROSS K_JOIN select_join_right_part
 ;

select_join_right_part
 : identifier
 | subquery
 ;

select_prewhere_step
 : K_PREWHERE expression_with_optional_alias
 ;

select_where_step
 : K_WHERE expression_with_optional_alias
 ;

select_groupby_step
 : K_GROUP K_BY not_empty_expression_list ( K_WITH K_TOTALS ) ?
 ;

select_having_step
 : K_HAVING expression_with_optional_alias
 ;

select_orderby_step
 : K_ORDER K_BY order_by_expression_list
 ;

select_limit_step
 : K_LIMIT NUMERIC_LITERAL ( COMMA NUMERIC_LITERAL )?
 ;

select_limitby_step
 : K_LIMIT NUMERIC_LITERAL K_BY not_empty_expression_list
 ;

select_settings_step
 : K_SETTINGS assignment_list
 ;

select_format_step
 : K_FORMAT identifier
 ;

insert_query
 :  K_INSERT K_INTO full_table_name
          ( K_ID ASSIGN STRING_LITERAL )? // wtf?
          ( LPAREN column_name_list RPAREN )?
          ( K_VALUES LPAREN literal (COMMA literal )* RPAREN(COMMA LPAREN literal (COMMA literal )* RPAREN)* // ch тут дальше не парсит. а я написал скобки
          | K_FORMAT format_name // ch тут дальше не парсит, только доедает все пробелы или один перевод строки. pushMode()
          | select_query )
 ;

create_query
 :  ( K_CREATE | K_ATTACH ) K_TEMPORARY?
            ( K_DATABASE ( K_IF K_NOT K_EXISTS ) ? database_name
            | K_TABLE ( K_IF K_NOT K_EXISTS ) ? full_table_name ( K_ON K_CLUSTER cluster_name ) ?
               ( LPAREN column_declaration_list RPAREN engine ( K_AS select_query ) ? // если VIEW - то есть и колонки и select.
               | engine K_AS (  select_query
                             |  full_table_name engine? // wtf
                             )
               )
            | K_MATERIALIZED? K_VIEW ( K_IF K_NOT K_EXISTS ) ? full_table_name
               ( LPAREN column_declaration_list RPAREN ) ? engine? K_POPULATE? K_AS select_query
            )
 ;

rename_query
 :  K_RENAME K_TABLE full_table_name K_TO full_table_name ( COMMA full_table_name K_TO full_table_name )* ( K_ON K_CLUSTER cluster_name ) ?
 ;

drop_query
 :  ( K_DROP | K_DETACH )
            ( K_DATABASE ( K_IF K_EXISTS ) ? database_name ( K_ON K_CLUSTER cluster_name ) ?
            | K_TABLE ( K_IF K_EXISTS ) ? full_table_name ( K_ON K_CLUSTER cluster_name ) ?
            )
 ;

alter_query
 : K_ALTER K_TABLE full_table_name ( K_ON K_CLUSTER cluster_name ) ?
        alter_query_element ( COMMA alter_query_element ) *
 ;

alter_query_element
 : K_ADD K_COLUMN compound_name_type_pair ( K_AFTER column_name ) ?
 | K_DROP K_COLUMN column_name
 | K_MODIFY K_COLUMN compound_name_type_pair
 | K_ATTACH K_PARTITION partition_name
 | K_DETACH K_PARTITION partition_name
 | K_DROP K_PARTITION partition_name
 | K_FETCH K_PARTITION partition_name K_FROM STRING_LITERAL
 | K_FREEZE K_PARTITION partition_name
 ;

clickhouse_type
    : simple_type
    | T_AGGREGATE_FUNCTION LPAREN function_name ( COMMA clickhouse_type ) * RPAREN
    | T_ARRAY LPAREN clickhouse_type RPAREN
    | T_TUPLE LPAREN clickhouse_type ( COMMA clickhouse_type ) * RPAREN
    | T_NULLABLE LPAREN clickhouse_type LPAREN
    ;

simple_type
    : T_UINT8
    | T_UINT16
    | T_UINT32
    | T_UINT64
    | T_INT8
    | T_INT16
    | T_INT32
    | T_INT64
    | T_FLOAT32
    | T_FLOAT64
    | T_ENUM8 LPAREN enum_entry ( COMMA enum_entry ) * LPAREN
    | T_ENUM16 LPAREN enum_entry ( COMMA enum_entry ) * LPAREN
    | T_UUID
    | T_DATE
    | T_DATETIME
    | T_STRING
    | T_INTERVAL_YEAR
    | T_INTERVAL_MONTH
    | T_INTERVAL_WEEK
    | T_INTERVAL_DAY
    | T_INTERVAL_HOUR
    | T_INTERVAL_MINUTE
    | T_INTERVAL_SECOND
    | T_NULL
    | T_FIXEDSTRING LPAREN NUMERIC_LITERAL LPAREN
    ;

enum_entry
    : STRING_LITERAL ASSIGN NUMERIC_LITERAL
    ;

use_query
 : K_USE database_name
 ;

set_query
 : K_SET K_GLOBAL? assignment_list
 ;

assignment_list
 : assignment ( COMMA assignment ) *
 ;

assignment
 : identifier ASSIGN literal
 ;

kill_query_query
 : K_KILL K_QUERY K_WHERE expression_with_optional_alias ( K_SYNC | K_ASYNC | K_TEST )
 ;

optimize_query
 : K_OPTIMIZE K_TABLE full_table_name ( K_PARTITION STRING_LITERAL ) ? K_FINAL?
 ;

table_properties_query
 : ( K_EXISTS | ( K_DESCRIBE | K_DESC ) | K_SHOW K_CREATE ) K_TABLE full_table_name query_outfile_step? ( K_FORMAT format_name ) ?
 ;

show_tables_query
 : K_SHOW ( K_DATABASES
            | K_TABLES ( K_FROM database_name ) ? ( K_NOT? K_LIKE STRING_LITERAL ) ? )
             query_outfile_step?
            ( K_FORMAT format_name ) ?
 ;

show_processlist_query
 : K_SHOW K_PROCESSLIST query_outfile_step? ( K_FORMAT format_name ) ?
 ;

check_query
 : K_CHECK K_TABLE full_table_name
 ;

// 2. QUERY ELEMENTS

full_table_name
 : ( database_name DOT ) ? table_name
 ;

partition_name
 : identifier | STRING_LITERAL
 ;

cluster_name
 : identifier | STRING_LITERAL
 ;

database_name
 :  identifier
 ;

table_name
 :  identifier
 ;

format_name
 :  identifier
 ;

query_outfile_step
 : K_INTO K_OUTFILE STRING_LITERAL
 ;

engine
 : K_ENGINE ASSIGN identifier_with_optional_parameters
 ;

identifier_with_optional_parameters
 :    identifier_with_parameters
 |    identifier
 ;

identifier_with_parameters
 : function
 | nested_table
 ;

order_by_expression_list
 :  order_by_element ( COMMA order_by_element ) *
 ;

order_by_element
 : expression_with_optional_alias ( K_DESC | K_DESCENDING | K_ASC | K_ASCENDING ) ? ( K_NULLS ( K_FIRST | K_LAST ) ) ? ( K_COLLATE STRING_LITERAL ) ?
 ;

nested_table
 :   identifier LPAREN name_type_pair_list RPAREN
 ;

name_type_pair_list
 :  name_type_pair ( COMMA name_type_pair ) *
 ;

name_type_pair
 : identifier column_type
 ;

compound_name_type_pair
 : compound_identifier column_type
 ;

column_declaration_list
 : column_declaration ( COMMA column_declaration ) *
 ;

column_declaration
 : column_name
      ( ( K_DEFAULT | K_MATERIALIZED | K_ALIAS ) expr
      | column_type
      )
 ;

column_name
 : identifier
 ;

column_type
 : clickhouse_type
 ;

column_name_list
 :  column_name ( COMMA column_name ) *
 ;

select_expr_list
 : select_expr ( COMMA select_expr) *
 ;

select_expr
 : expr select_alias?
 ;

select_alias
 : K_AS? alias_name
 ;

alias
 : K_AS alias_name
 ;

alias_name
 : identifier
 ;

table_function
 :   function
 ;


subquery
 :  LPAREN select_query_main RPAREN
 ;

expression_with_optional_alias
 : expr alias?
 ;

//  EXPRESSIONS

expr
 :  LPAREN expr RPAREN                                                                                                                                              # ExprParen
 |  function                                                                                                                                                        # ExprFunction
 |  K_CASE expr? ( K_WHEN expr K_THEN expr ) ( K_WHEN expr K_THEN expr ) * K_ELSE expr K_END                                                                        # ExprCase
 |  expr DOT expr                                                                                                                                                   # ExprTupleElement
 |  expr LBRAKET expr RBRAKET                                                                                                                                       # ExprArrayElement
 |  MINUS expr                                                                                                                                                      # ExprUnaryMinus
 |  K_CAST LPAREN expr K_AS clickhouse_type RPAREN                                                                                                                  # ExprCast
 |  expr ( STAR | DIVIDE | PERCENT ) expr                                                                                                                           # ExprMul
 |  expr  ( PLUS | MINUS ) expr                                                                                                                                     # ExprAdd
 |  expr  CONCAT expr                                                                                                                                               # ExprConcat
 |  expr  K_BETWEEN expr K_AND expr                                                                                                                                 # ExprBetween
 |  expr ( EQUALS | ASSIGN | NOT_EQUALS | NOT_EQUALS2 | LE | GE | LT | GT | K_LIKE | K_NOT K_LIKE ) expr                                                            # ExprLogical
 |  expr ( K_IN | K_NOT K_IN | K_GLOBAL K_IN | K_GLOBAL K_NOT K_IN ) expr                                                                                           # ExprIn
 |  expr ( K_IS K_NULL | K_IS K_NOT K_NULL )                                                                                                                        # ExprIsNull
 |  K_INTERVAL expr interval_unit                                                                                                                                   # ExprInterval
 |  K_NOT expr                                                                                                                                                      # ExprNot
 |  expr K_AND expr                                                                                                                                                 # ExprAnd
 |  expr K_OR expr                                                                                                                                                  # ExprOr
 |  expr QUESTION expr COLON expr                                                                                                                                   # ExprTernary
 |  ( LPAREN identifier ( COMMA identifier )* RPAREN | identifier ( COMMA identifier )* ) RARROW expr                                                               # ExprLambda
 |  subquery                                                                                                                                                        # ExprSubquery
 |  LPAREN  not_empty_expression_list RPAREN                                                                                                                        # ExprList
 |  array                                                                                                                                                           # ExprArray
 |  literal                                                                                                                                                         # ExprLiteral
 |  compound_identifier                                                                                                                                             # ExprId
 |  STAR                                                                                                                                                            # ExprStar
 | expr alias                                                                                                                                                       # ExprWithAlias
 ;

interval_unit
 : K_YEAR
 | K_MONTH
 | K_WEEK
 | K_DAY
 | K_HOUR
 | K_MINUTE
 | K_SECOND
 ;
expression_list
 :   ( not_empty_expression_list )?
 ;

not_empty_expression_list
 : expr ( COMMA expr )*
 ;

array
 :   LBRAKET expression_list RBRAKET
 ;

function
 : function_name function_parameters? function_arguments
 ;

function_parameters
 : LPAREN ( expr ( COMMA expr )* )? RPAREN
 ;
function_arguments
 : LPAREN ( expr ( COMMA expr )* )? RPAREN
 ;

function_name
 : identifier
 ;

identifier
 : QUOTED_LITERAL
 | IDENTIFIER
    // в данном случае мы разрешаем ключевым словам выступать в качестве имен колонок или функций.
 | simple_type
 | keyword
 ;

keyword
 : K_ADD
 | K_AFTER
 | K_ALL
 | K_ALIAS
 | K_ALTER
 | K_AND
 | K_ANY
 | K_ARRAY
 | K_AS
 | K_ASCENDING
 | K_ASC
 | K_ASYNC
 | K_ATTACH
 | K_BETWEEN
 | K_BY
 | K_CASE
 | K_CHECK
 | K_COLUMN
 | K_COLLATE
 | K_CREATE
 | K_CROSS
 | K_DESCRIBE
 | K_DESCENDING
 | K_DESC
 | K_DATABASE
 | K_DATABASES
 | K_DEFAULT
 | K_DETACH
 | K_DISTINCT
 | K_DROP
 | K_ENGINE
 | K_ELSE
 | K_END
 | K_EXISTS
 | K_FINAL
 | K_FIRST
 | K_FROM
 | K_FORMAT
 | K_FULL
 | K_GLOBAL
 | K_GROUP
 | K_HAVING
 | K_ID
 | K_IF
 | K_INNER
 | K_INSERT
 | K_INTO
 | K_IN
 | K_IS
 | K_JOIN
 | K_KILL
 | K_LAST
 | K_LEFT
 | K_LIKE
 | K_LIMIT
 | K_MAIN
 | K_MATERIALIZED
 | K_MODIFY
 | K_NOT
 | K_NULL
 | K_NULLS
 | K_OFFSET
 | K_ON
 | K_OPTIMIZE
 | K_ORDER
 | K_OR
 | K_OUTFILE
 | K_PARTITION
 | K_POPULATE
 | K_PREWHERE
 | K_PROCESSLIST
 | K_QUERY
 | K_RENAME
 | K_RETURN
 | K_RIGHT
 | K_SAMPLE
 | K_SELECT
 | K_SET
 | K_SETTINGS
 | K_SHOW
 | K_SYNC
 | K_TABLE
 | K_TABLES
 | K_TEMPORARY
 | K_TEST
 | K_THEN
 | K_TOTALS
 | K_TO
 | K_OUTER
 | K_VALUES
 | K_VIEW
 | K_UNION
 | K_USE
 | K_USING
 | K_WHEN
 | K_WHERE
 | K_WITH
 ;

compound_identifier
: identifier DOT identifier
| identifier
;


literal
 :    K_NULL
 |    NUMERIC_LITERAL
 |    STRING_LITERAL
 ;

err
 : UNEXPECTED_CHAR
   {
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
   }
 ;

