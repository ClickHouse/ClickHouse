-- netloc() returns the network location `username:password@host:port` of a URL.
-- Expected behaviour source: the FunctionDocumentation block of netloc in
-- src/Functions/URL/netloc.cpp. Covered here: the restricted symbols that make
-- the scheme scan give up, the sub-delimiters that stay inside the userinfo,
-- and the '?' and '#' delimiters that end the network location.

SELECT '-- a restricted symbol inside the scheme: no network location';
SELECT 'space' AS symbol, netloc('ft p://localhost') AS netloc;
SELECT 'tab' AS symbol, netloc('ft\tp://localhost') AS netloc;
SELECT 'less-than' AS symbol, netloc('ft<p://localhost') AS netloc;
SELECT 'greater-than' AS symbol, netloc('ft>p://localhost') AS netloc;
SELECT 'percent' AS symbol, netloc('ft%p://localhost') AS netloc;
SELECT 'brace-open' AS symbol, netloc('ft{p://localhost') AS netloc;
SELECT 'brace-close' AS symbol, netloc('ft}p://localhost') AS netloc;
SELECT 'pipe' AS symbol, netloc('ft|p://localhost') AS netloc;
SELECT 'backslash' AS symbol, netloc('ft\\p://localhost') AS netloc;
SELECT 'caret' AS symbol, netloc('ft^p://localhost') AS netloc;
SELECT 'tilde' AS symbol, netloc('ft~p://localhost') AS netloc;
SELECT 'bracket-open' AS symbol, netloc('ft[p://localhost') AS netloc;
SELECT 'bracket-close' AS symbol, netloc('ft]p://localhost') AS netloc;
SELECT 'semicolon' AS symbol, netloc('ft;p://localhost') AS netloc;
SELECT 'equals' AS symbol, netloc('ft=p://localhost') AS netloc;
SELECT 'ampersand' AS symbol, netloc('ft&p://localhost') AS netloc;

SELECT '-- a sub-delimiter before the @ belongs to the userinfo';
SELECT netloc('ftp://user;name@localhost/') AS netloc;
SELECT netloc('ftp://localhost;p') AS netloc;

SELECT '-- the query string ends the network location';
SELECT netloc('ftp://user:pass@localhost?q=1') AS netloc;

SELECT '-- the fragment ends the network location';
SELECT netloc('ftp://localhost#frag') AS netloc;
SELECT netloc('ftp://user@localhost#frag') AS netloc;
