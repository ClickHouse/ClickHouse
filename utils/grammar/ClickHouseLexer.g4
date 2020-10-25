lexer grammar ClickHouseLexer;

LINE_COMMENT
	:	'--' ~[\r\n]*  -> channel(HIDDEN)
	;

 // TOKENS, KEYWORDS

K_ADD : A D D;
K_AFTER : A F T E R;
K_ALL : A L L;
K_ALIAS : A L I A S;
K_ALTER : A L T E R;
K_AND : A N D;
K_ANY : A N Y;
K_ARRAY : A R R A Y;
K_AS : A S;
K_ASCENDING : A S C E N D I N G;
K_ASC : A S C;
K_ASYNC : A S Y N C;
K_ATTACH : A T T A C H;
K_BETWEEN : B E T W E E N;
K_BY : B Y;
K_CASE : C A S E;
K_CAST : C A S T;
K_CHECK : C H E C K;
K_CLUSTER : C L U S T E R;
K_COLUMN : C O L U M N;
K_COLLATE : C O L L A T E;
K_CREATE : C R E A T E;
K_CROSS : C R O S S;
K_DAY : D A Y;
K_DESCRIBE : D E S C R I B E;
K_DESCENDING : D E S C E N D I N G;
K_DESC : D E S C;
K_DATABASE : D A T A B A S E;
K_DATABASES : D A T A B A S E S;
K_DEFAULT : D E F A U L T;
K_DETACH : D E T A C H;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_ELSE : E L S E;
K_END : E N D;
K_ENGINE : E N G I N E;
K_EXISTS : E X I S T S;
K_FETCH : F E T C H;
K_FINAL : F I N A L;
K_FIRST : F I R S T;
K_FROM : F R O M;
K_FREEZE : F R E E Z E;
K_FORMAT : F O R M A T;
K_FULL : F U L L;
K_GLOBAL : G L O B A L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_HOUR : H O U R;
K_ID : I D;
K_IF : I F;
K_INNER : I N N E R;
K_INSERT : I N S E R T;
K_INTERVAL : I N T E R V A L;
K_INTO : I N T O;
K_IN : I N;
K_IS : I S;
K_JOIN : J O I N;
K_KILL: K I L L;
K_LAST : L A S T;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_MAIN : M A I N;  // not a clickhouse reverved word
K_MATERIALIZED : M A T E R I A L I Z E D;
K_MINUTE : M I N U T E;
K_MODIFY : M O D I F Y;
K_MONTH : M O N T H;
K_NOT : N O T;
K_NULL : N U L L;
K_NULLS : N U L L S;
K_OFFSET : O F F S E T;
K_ON : O N;
K_OPTIMIZE : O P T I M I Z E;
K_ORDER : O R D E R;
K_OR : O R;
K_OUTFILE : O U T F I L E;
K_PARTITION : P A R T I T I O N;
K_POPULATE : P O P U L A T E;
K_PREWHERE : P R E W H E R E;
K_PROCESSLIST : P R O C E S S L I S T;
K_QUERY : Q U E R Y;
K_RENAME : R E N A M E;
K_RETURN : R E T U R N;  // not a clickhouse reverved word
K_RIGHT : R I G H T;
K_SAMPLE : S A M P L E;
K_SECOND : S E C O N D;
K_SELECT : S E L E C T;
K_SET : S E T;
K_SETTINGS : S E T T I N G S;
K_SHOW : S H O W;
K_SYNC : S Y N C;
K_TABLE : T A B L E;
K_TABLES : T A B L E S;
K_TEMPORARY : T E M P O R A R Y;
K_TEST : T E S T;
K_THEN : T H E N;
K_TOTALS : T O T A L S;
K_TO : T O;
K_OUTER: O U T E R;
K_VALUES : V A L U E S;
K_VIEW : V I E W;
K_UNION : U N I O N;
K_USE : U S E;
K_USING : U S I N G;
K_WEEK : W E E K;
K_WHEN : W H E N;
K_WHERE : W H E R E;
K_WITH : W I T H;
K_YEAR : Y E A R;

COLON        : ':'                    ;
COMMA        : ','                    ;
SEMI         : ';'                    ;
LPAREN       : '('                    ;
RPAREN       : ')'                    ;
RARROW       : '->'                   ;
LT           : '<'                    ;
GT           : '>'                    ;
QUESTION     : '?'                    ;
STAR         : '*'                    ;
PLUS         : '+'                    ;
CONCAT       : '||'                   ;
OR           : '|'                    ;
DOLLAR       : '$'                    ;
DOT		     : '.'                    ;
PERCENT      : '%'                    ;
MINUS        : '-'                    ;
DIVIDE       : '/'                    ;
EQUALS       : '=='                   ;
ASSIGN       : '='                    ;
NOT_EQUALS   : '!='                   ;
NOT_EQUALS2  : '<>'                   ;
LE           : '<='                   ;
GE           : '>='                   ;
LBRAKET      : '['                    ;
RBRAKET      : ']'                    ;
LCURLY       : '{'                    ;
RCURLY       : '}'                    ;


T_ARRAY : 'Array' ;
T_TUPLE : 'Tuple' ;
T_NULLABLE : 'Nullable' ;
T_FLOAT32 : 'Float32' ;
T_FLOAT64 : 'Float64' ;
T_UINT8 : 'UInt8' ;
T_UINT16 : 'UInt16' ;
T_UINT32 : 'UInt32' ;
T_UINT64 : 'UInt64' ;
T_INT8 : 'Int8' ;
T_INT16 : 'Int16' ;
T_INT32 : 'Int32' ;
T_INT64 : 'Int64' ;
T_ENUM8 : 'Enum8' ;
T_ENUM16 : 'Enum16' ;
T_UUID : 'UUID' ;
T_DATE : 'Date' ;
T_DATETIME : 'DateTime' ;
T_STRING : 'String' ;
T_FIXEDSTRING : 'FixedString' ;
T_NULL : 'Null' ;
T_INTERVAL_YEAR : 'IntervalYear' ;
T_INTERVAL_MONTH : 'IntervalMonth' ;
T_INTERVAL_WEEK : 'IntervalWeek' ;
T_INTERVAL_DAY : 'IntervalDay' ;
T_INTERVAL_HOUR : 'IntervalHour' ;
T_INTERVAL_MINUTE : 'IntervalMinute' ;
T_INTERVAL_SECOND : 'IntervalSecond' ;
T_AGGREGATE_FUNCTION : 'AggregateFunction' ;
// lambda type has unknown name.

IDENTIFIER
  : [a-zA-Z_] [a-zA-Z_0-9]*
  ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\\\'' )* '\''
 ;

QUOTED_LITERAL
 : '`' ( ~'`' )* '`'
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
