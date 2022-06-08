SET sql_dialect='mysql';

-- now multiqueries and comments are processed manually and separately
-- so let's check that it works correctly

/* comment; SELECT 0 */ SELECT 1; --comment SELECT 0; # // should not be executed
SELECT '1; SELECT // # 0;;;; SELECT 0'; // comment lol
SELECT "1; SELECT /* 'str';;;; SELECT 'str' 0"; // comment lol
SELECT /* comment */2;

-- okey, comments and multiqueries are OK, lets check simple SELECTs

/*
	numbers and arithmetics
*/
SELECT 'MOO: numbers and arithmetics';
SELECT 1;
SELECT 1 + 1;
SELECT (1 + 1) * (1 + 1);
SELECT 3.14;
SELECT -(1 / 2 + 1/4 + 1/8 + 1/16);
SELECT 12345 % 10;
SELECT 12345 mod 10;
SELECT 12345 div 10; -- integer division
SELECT 12345 / 10; -- real division
SELECT  ((12345) mod 10),
		((12345 div 10) mod 10),
		((12345 div 100) mod 10),
		((12345 div 1000) mod 10),
		((12345 div 10000) mod 10);

/*
	comparisons
*/
SELECT 'MOO: comparisons';
SELECT 3 * 4 = 2 * 6;
SELECT (3 * 4) >= (2 * 6);
SELECT (3 * 4) <= (2 * 6);
SELECT (3 * 4) + 1 > (2 * 6);
SELECT (3 * 4) - 1 < (2 * 6);
SELECT (3 * 4) - 1 != (2 * 6);
SELECT (3 * 4) - 1 <> (2 * 6);

/*
	strings
*/
SELECT 'MOO: strings';
SELECT "double quoted string literal";
SELECT 'ordinary quoted string literal';
SELECT "aba" = 'aba';
SELECT 'aba' <> "abc";
SELECT 'aba' < 'abc';

-- string concatenation
SELECT '1' '2' '3';
SELECT "a" 'b' "c" 'd';

/* 
	boolean 
*/
SELECT 'MOO: boolean';
SELECT FALSE;
SELECT TRUE;
SELECT NULL;
SELECT \N;
SELECT TRUE AND TRUE;
SELECT TRUE AND FALSE;
SELECT FALSE OR FALSE;
SELECT FALSE OR TRUE;
SELECT FALSE XOR TRUE;
SELECT FALSE XOR FALSE;
SELECT NOT TRUE;
SELECT NOT FALSE;

-- alternative AND & OR
SELECT FALSE && TRUE;
SELECT FALSE || TRUE;

-- order of operations
SELECT ((3 * 4) = (2 * 6) OR (0 = 1)) AND FALSE;
SELECT (3 * 4) = (2 * 6) OR (0 = 1) AND FALSE;
SELECT NOT TRUE OR TRUE;
SELECT NOT (TRUE OR TRUE);

-- De Morgan's law
SELECT (NOT (TRUE OR TRUE)) = (NOT TRUE AND NOT TRUE); 
SELECT (NOT (TRUE OR FALSE)) = (NOT TRUE AND NOT FALSE); 
SELECT (NOT (FALSE OR TRUE)) = (NOT FALSE AND NOT TRUE); 
SELECT (NOT (FALSE OR FALSE)) = (NOT FALSE AND NOT FALSE); 

-- NULL safe equals operator (now it is handled by AST converter)
SELECT 1 <=> 1;
SELECT 1 <=> 2;
SELECT 1 <=> NULL;
SELECT NULL <=> NULL;

SELECT "MOO: end of MySQL queries";

SET sql_dialect='clickhouse'; -- paranoid
