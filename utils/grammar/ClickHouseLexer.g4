lexer grammar ClickHouseLexer;

// Comments and whitespace

LINE_COMMENT: '--' ~('\n'|'\r')* ('\n' | '\r' | EOF) -> skip;
WHITESPACE: [ \u000B\u000C\t\r\n] -> skip;

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
fragment DIGIT: [0-9];

ARROW: '->';
ASTERISK: '*';
BACKQUOTE: '`';
BACKSLASH: '\\';
COLON: ':';
COMMA: ',';
CONCAT: '||';
DASH: '-';
DOT: '.';
EQ: '=' | '==';
GE: '>=';
GT: '>';
LBRACKET: '[';
LE: '<=';
LPAREN: '(';
LT: '<';
NOT_EQ: '!=' | '<>';
PERCENT: '%';
PLUS: '+';
QUERY: '?';
QUOTE_SINGLE: '\'';
RBRACKET: ']';
RPAREN: ')';
SEMICOLON: ';';
SLASH: '/';
UNDERSCORE: '_';

// Tokens

IDENTIFIER: (LETTER | UNDERSCORE) (LETTER | UNDERSCORE | DIGIT)*;
LITERAL: NUMBER_LITERAL | STRING_LITERAL | NULL;
NUMBER_LITERAL: DIGIT+; // Unsigned natural integer with meaningless leading zeroes. TODO: don't forget exponential repr.
STRING_LITERAL: QUOTE_SINGLE ( ~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .) )* QUOTE_SINGLE;

// Keywords

ALL: A L L;
AND: A N D;
ARRAY: A R R A Y;
AS: A S;
ASCENDING: A S C | A S C E N D I N G;
BETWEEN: B E T W E E N;
BOTH: B O T H;
BY: B Y;
CASE: C A S E;
CAST: C A S T;
COLLATE: C O L L A T E;
DAY: D A Y;
DESCENDING: D E S C | D E S C E N D I N G;
DISTINCT: D I S T I N C T;
ELSE: E L S E;
END: E N D;
EXTRACT: E X T R A C T;
FINAL: F I N A L;
FIRST: F I R S T;
FORMAT: F O R M A T;
FROM: F R O M;
GLOBAL: G L O B A L;
GROUP: G R O U P;
HAVING: H A V I N G;
HOUR: H O U R;
IN: I N;
INTERVAL: I N T E R V A L;
INTO: I N T O;
IS: I S;
JOIN: J O I N;
LAST: L A S T;
LEADING: L E A D I N G;
LEFT: L E F T;
LIKE: L I K E;
LIMIT: L I M I T;
MINUTE: M I N U T E;
MONTH: M O N T H;
NOT: N O T;
NULL: N U L L;
NULLS: N U L L S;
OFFSET: O F F S E T;
OR: O R;
ORDER: O R D E R;
OUTFILE: O U T F I L E;
PREWHERE: P R E W H E R E;
QUARTER: Q U A R T E R;
SAMPLE: S A M P L E;
SECOND: S E C O N D;
SELECT: S E L E C T;
SETTINGS: S E T T I N G S;
THEN: T H E N;
TOTALS: T O T A L S;
TRAILING: T R A I L I N G;
TRIM: T R I M;
UNION: U N I O N;
WEEK: W E E K;
WHEN: W H E N;
WHERE: W H E R E;
WITH: W I T H;
YEAR: Y E A R;

// Interval types
INTERVAL_TYPE: SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR;
