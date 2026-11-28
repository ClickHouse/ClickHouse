-- `ParserDictionary` reads the property clauses of a dictionary definition (SOURCE, LIFETIME,
-- LAYOUT, RANGE and SETTINGS) in a loop, so a definition may give them in any order after
-- `PRIMARY KEY` (src/Parsers/ParserDictionary.cpp). Each clause is a keyword, an opening bracket,
-- the body of the clause and a closing bracket. A definition that breaks one of those three steps
-- must be rejected as a syntax error, not accepted with the clause silently dropped: the server
-- stores a dictionary definition in formatted form and parses it again at the next start, so a
-- definition that lost a clause here would come back as a different dictionary.
-- Grammar: docs/reference/statements/create/dictionary.mdx ("Syntax").
-- Nothing is created: the well-formed definitions are only formatted again.

-- The order of the clauses does not change the definition. The three statements below give the five
-- clauses in the documented order, in the reverse order and interleaved; all three print the one
-- spelling that the server would store.
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, b UInt64, e UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(RANGE_HASHED()) RANGE(MIN b MAX e) LIFETIME(MIN 0 MAX 300) SETTINGS(max_threads = 1)$$);
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, b UInt64, e UInt64) PRIMARY KEY id SETTINGS(max_threads = 1) LIFETIME(MIN 0 MAX 300) RANGE(MIN b MAX e) LAYOUT(RANGE_HASHED()) SOURCE(NULL())$$);
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, b UInt64, e UInt64) PRIMARY KEY id LAYOUT(RANGE_HASHED()) SOURCE(NULL()) SETTINGS(max_threads = 1) RANGE(MIN b MAX e) LIFETIME(MIN 0 MAX 300)$$);

-- The key may be a bracketed list of columns, which is formatted without the brackets.
SELECT formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, k String) PRIMARY KEY (id, k) SOURCE(NULL()) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)$$);

-- The key is a list of column names; a literal is not one of them.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY (1) LAYOUT(FLAT()); -- { clientError SYNTAX_ERROR }
-- A bracketed key that is never closed: the next clause does not end it.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY (id LAYOUT(FLAT())); -- { clientError SYNTAX_ERROR }

-- SOURCE: the keyword without its brackets, a body that is not a named list of parameters, and a
-- body that is not followed by the closing bracket.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE NULL(); -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(123); -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(NULL() LAYOUT(FLAT())); -- { clientError SYNTAX_ERROR }

-- LIFETIME: the keyword without its brackets, and a value that is not followed by the closing
-- bracket.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id LIFETIME 300; -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id LIFETIME(300 LAYOUT(FLAT())); -- { clientError SYNTAX_ERROR }

-- LAYOUT: the keyword without its brackets, and a layout that is not followed by the closing
-- bracket.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id LAYOUT FLAT(); -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id LAYOUT(FLAT() LIFETIME(300)); -- { clientError SYNTAX_ERROR }

-- RANGE: the keyword without its brackets, a body that is not a MIN/MAX pair of column names, and a
-- pair that is not followed by the closing bracket.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id RANGE MIN b MAX e; -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id RANGE(123); -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id RANGE(MIN b MAX e 5); -- { clientError SYNTAX_ERROR }

-- SETTINGS: the keyword without its brackets, a body that is not a name-value pair, and a pair that
-- is not followed by the closing bracket.
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SETTINGS max_threads = 1; -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SETTINGS(123); -- { clientError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SETTINGS(max_threads = 1 LAYOUT(FLAT())); -- { clientError SYNTAX_ERROR }
