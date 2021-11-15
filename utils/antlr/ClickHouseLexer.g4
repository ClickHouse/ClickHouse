lexer grammar ClickHouseLexer;

// NOTE: don't forget to add new keywords to the parser rule "keyword"!

// Keywords

ADD: A D D;
AFTER: A F T E R;
ALIAS: A L I A S;
ALL: A L L;
ALTER: A L T E R;
AND: A N D;
ANTI: A N T I;
ANY: A N Y;
ARRAY: A R R A Y;
AS: A S;
ASCENDING: A S C | A S C E N D I N G;
ASOF: A S O F;
AST: A S T;
ASYNC: A S Y N C;
ATTACH: A T T A C H;
BETWEEN: B E T W E E N;
BOTH: B O T H;
BY: B Y;
CASE: C A S E;
CAST: C A S T;
CHECK: C H E C K;
CLEAR: C L E A R;
CLUSTER: C L U S T E R;
CODEC: C O D E C;
COLLATE: C O L L A T E;
COLUMN: C O L U M N;
COMMENT: C O M M E N T;
CONSTRAINT: C O N S T R A I N T;
CREATE: C R E A T E;
CROSS: C R O S S;
CUBE: C U B E;
CURRENT: C U R R E N T;
DATABASE: D A T A B A S E;
DATABASES: D A T A B A S E S;
DATE: D A T E;
DAY: D A Y;
DEDUPLICATE: D E D U P L I C A T E;
DEFAULT: D E F A U L T;
DELAY: D E L A Y;
DELETE: D E L E T E;
DESC: D E S C;
DESCENDING: D E S C E N D I N G;
DESCRIBE: D E S C R I B E;
DETACH: D E T A C H;
DICTIONARIES: D I C T I O N A R I E S;
DICTIONARY: D I C T I O N A R Y;
DISK: D I S K;
DISTINCT: D I S T I N C T;
DISTRIBUTED: D I S T R I B U T E D;
DROP: D R O P;
ELSE: E L S E;
END: E N D;
ENGINE: E N G I N E;
EVENTS: E V E N T S;
EXISTS: E X I S T S;
EXPLAIN: E X P L A I N;
EXPRESSION: E X P R E S S I O N;
EXTRACT: E X T R A C T;
FETCHES: F E T C H E S;
FINAL: F I N A L;
FIRST: F I R S T;
FLUSH: F L U S H;
FOLLOWING: F O L L O W I N G;
FOR: F O R;
FORMAT: F O R M A T;
FREEZE: F R E E Z E;
FROM: F R O M;
FULL: F U L L;
FUNCTION: F U N C T I O N;
GLOBAL: G L O B A L;
GRANULARITY: G R A N U L A R I T Y;
GROUP: G R O U P;
HAVING: H A V I N G;
HIERARCHICAL: H I E R A R C H I C A L;
HOUR: H O U R;
ID: I D;
IF: I F;
ILIKE: I L I K E;
IN: I N;
INDEX: I N D E X;
INF: I N F | I N F I N I T Y;
INJECTIVE: I N J E C T I V E;
INNER: I N N E R;
INSERT: I N S E R T;
INTERVAL: I N T E R V A L;
INTO: I N T O;
IS: I S;
IS_OBJECT_ID: I S UNDERSCORE O B J E C T UNDERSCORE I D;
JOIN: J O I N;
KEY: K E Y;
KILL: K I L L;
LAST: L A S T;
LAYOUT: L A Y O U T;
LEADING: L E A D I N G;
LEFT: L E F T;
LIFETIME: L I F E T I M E;
LIKE: L I K E;
LIMIT: L I M I T;
LIVE: L I V E;
LOCAL: L O C A L;
LOGS: L O G S;
MATERIALIZE: M A T E R I A L I Z E;
MATERIALIZED: M A T E R I A L I Z E D;
MAX: M A X;
MERGES: M E R G E S;
MIN: M I N;
MINUTE: M I N U T E;
MODIFY: M O D I F Y;
MONTH: M O N T H;
MOVE: M O V E;
MUTATION: M U T A T I O N;
NAN_SQL: N A N; // conflicts with macro NAN
NO: N O;
NOT: N O T;
NULL_SQL: N U L L; // conflicts with macro NULL
NULLS: N U L L S;
OFFSET: O F F S E T;
ON: O N;
OPTIMIZE: O P T I M I Z E;
OR: O R;
ORDER: O R D E R;
OUTER: O U T E R;
OUTFILE: O U T F I L E;
OVER: O V E R;
PARTITION: P A R T I T I O N;
POPULATE: P O P U L A T E;
PRECEDING: P R E C E D I N G;
PREWHERE: P R E W H E R E;
PRIMARY: P R I M A R Y;
PROJECTION: P R O J E C T I O N;
QUARTER: Q U A R T E R;
RANGE: R A N G E;
RELOAD: R E L O A D;
REMOVE: R E M O V E;
RENAME: R E N A M E;
REPLACE: R E P L A C E;
REPLICA: R E P L I C A;
REPLICATED: R E P L I C A T E D;
RIGHT: R I G H T;
ROLLUP: R O L L U P;
ROW: R O W;
ROWS: R O W S;
SAMPLE: S A M P L E;
SECOND: S E C O N D;
SELECT: S E L E C T;
SEMI: S E M I;
SENDS: S E N D S;
SET: S E T;
SETTINGS: S E T T I N G S;
SHOW: S H O W;
SOURCE: S O U R C E;
START: S T A R T;
STOP: S T O P;
SUBSTRING: S U B S T R I N G;
SYNC: S Y N C;
SYNTAX: S Y N T A X;
SYSTEM: S Y S T E M;
TABLE: T A B L E;
TABLES: T A B L E S;
TEMPORARY: T E M P O R A R Y;
TEST: T E S T;
THEN: T H E N;
TIES: T I E S;
TIMEOUT: T I M E O U T;
TIMESTAMP: T I M E S T A M P;
TO: T O;
TOP: T O P;
TOTALS: T O T A L S;
TRAILING: T R A I L I N G;
TRIM: T R I M;
TRUNCATE: T R U N C A T E;
TTL: T T L;
TYPE: T Y P E;
UNBOUNDED: U N B O U N D E D;
UNION: U N I O N;
UPDATE: U P D A T E;
USE: U S E;
USING: U S I N G;
UUID: U U I D;
VALUES: V A L U E S;
VIEW: V I E W;
VOLUME: V O L U M E;
WATCH: W A T C H;
WEEK: W E E K;
WHEN: W H E N;
WHERE: W H E R E;
WINDOW: W I N D O W;
WITH: W I T H;
YEAR: Y E A R | Y Y Y Y;

JSON_FALSE: 'false';
JSON_TRUE: 'true';

// Tokens

IDENTIFIER
    : (LETTER | UNDERSCORE) (LETTER | UNDERSCORE | DEC_DIGIT)*
    | BACKQUOTE ( ~([\\`]) | (BACKSLASH .) | (BACKQUOTE BACKQUOTE) )* BACKQUOTE
    | QUOTE_DOUBLE ( ~([\\"]) | (BACKSLASH .) | (QUOTE_DOUBLE QUOTE_DOUBLE) )* QUOTE_DOUBLE
    ;
FLOATING_LITERAL
    : HEXADECIMAL_LITERAL DOT HEX_DIGIT* (P | E) (PLUS | DASH)? DEC_DIGIT+
    | HEXADECIMAL_LITERAL (P | E) (PLUS | DASH)? DEC_DIGIT+
    | DECIMAL_LITERAL DOT DEC_DIGIT* E (PLUS | DASH)? DEC_DIGIT+
    | DOT DECIMAL_LITERAL E (PLUS | DASH)? DEC_DIGIT+
    | DECIMAL_LITERAL E (PLUS | DASH)? DEC_DIGIT+
    ;
OCTAL_LITERAL: '0' OCT_DIGIT+;
DECIMAL_LITERAL: DEC_DIGIT+;
HEXADECIMAL_LITERAL: '0' X HEX_DIGIT+;

// It's important that quote-symbol is a single character.
STRING_LITERAL: QUOTE_SINGLE ( ~([\\']) | (BACKSLASH .) | (QUOTE_SINGLE QUOTE_SINGLE) )* QUOTE_SINGLE;

// Alphabet and allowed symbols

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

fragment LETTER: [a-zA-Z];
fragment OCT_DIGIT: [0-7];
fragment DEC_DIGIT: [0-9];
fragment HEX_DIGIT: [0-9a-fA-F];

ARROW: '->';
ASTERISK: '*';
BACKQUOTE: '`';
BACKSLASH: '\\';
COLON: ':';
COMMA: ',';
CONCAT: '||';
DASH: '-';
DOT: '.';
EQ_DOUBLE: '==';
EQ_SINGLE: '=';
GE: '>=';
GT: '>';
LBRACE: '{';
LBRACKET: '[';
LE: '<=';
LPAREN: '(';
LT: '<';
NOT_EQ: '!=' | '<>';
PERCENT: '%';
PLUS: '+';
QUERY: '?';
QUOTE_DOUBLE: '"';
QUOTE_SINGLE: '\'';
RBRACE: '}';
RBRACKET: ']';
RPAREN: ')';
SEMICOLON: ';';
SLASH: '/';
UNDERSCORE: '_';

// Comments and whitespace

MULTI_LINE_COMMENT: '/*' .*? '*/' -> skip;
SINGLE_LINE_COMMENT: '--' ~('\n'|'\r')* ('\n' | '\r' | EOF) -> skip;
WHITESPACE: [ \u000B\u000C\t\r\n] -> skip;  // '\n' can be part of multiline single query
