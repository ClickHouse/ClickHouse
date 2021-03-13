---
title: 'Fuzzing ClickHouse'
image: 'https://blog-images.clickhouse.tech/en/2021/fuzzing-clickhouse/some-checks-were-not-successful.png'
date: '2021-03-11'
author: '[Alexander Kuzmenkov](https://github.com/akuzm)'
tags: ['fuzzing', 'testing']
---

Testing is a major problem in software development: there is never enough of it. It becomes especially true for a database management system, whose task is to interpret a query language that works on the persistent state managed by the system in a distributed fashion. Each of these three functions is hard enough to test even in isolation, and it gets much worse when you combine them. As ClickHouse developers, we know this from experience. Despite a large amount of automated testing of all kinds we routinely perform as part of our continuous integration system, new bugs and regressions are creeping in. We are always looking for the ways to improve our test coverage, and this article will describe our recent development in this area &mdash; the AST-based query fuzzer.

## How to Test a SQL DBMS

A natural form of testing for a SQL DBMS is to create a SQL script describing the test case, and record its reference result. To test, we run the script and check that the result matches the reference. This is used in many SQL DBMS, and it is the default kind of a test you are expected to write for any ClickHouse feature or fix. Currently we have [73k lines of SQL tests alone](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless), that reach the [code coverage of 76%](https://clickhouse-test-reports.s3.yandex.net/0/47d684a5c35410201d4dd4f63f3287bf25cdabb7/coverage_report/test_output/index.html).

This form of testing, where a developer writes a few simplified examples of how the feature can and cannot be used, is sometimes called "example-based testing". Sadly, the bugs often appear in various corner cases and intersections of features, and it is not practical to enumerate all of these cases by hand. There is a technique for automating this process, called "property-based testing". It lets you write more general tests of the form "for all values matching these specs, the result of some operation on them should match this other spec". For example, such a test can check that if you add two positive numbers, the result is greater than both of them. But you don't specify which numbers exactly, only these properties. Then, the property testing system randomly generates some examples with particular numbers that match the specification, and checks that the result also matches its specification.

Property-based testing is said to be very efficient, but requires some developer effort and expertise to write the tests in a special way. There is another well-known testing technique that is in some sense a corner case of property-based testing, and that doesn't require much developer time. It is called fuzzing. When you are fuzzing your program, you feed it random inputs generated according to some grammar, and the property you are checking is that your program terminates correctly (no segfaults or assertions or other kinds of program errors). Most often, the grammar of input for fuzzing is simple &mdash; say, bit flips and additions, or maybe some dictionary. The space of possible inputs is huge, so to find interesting paths in it, fuzzing software records the code paths taken by the program under test for a particular input, and focuses on the inputs that lead to new code paths that were not seen before. It also employs some techniques for finding interesting constant values, and so on. In general, fuzzing allows you to find many interesting corner cases in your program automatically, without much developer involvement.

Generating valid SQL queries with bit flips would take a long time, so there are systems that generate queries based on the SQL grammar, such as [SQLSmith](https://github.com/anse1/sqlsmith).  They are succesfully used for finding bugs in databases. It would be interesting to use such a system for ClickHouse, but it requires some up-front effort to support the ClickHouse SQL grammar and functions, which may be different from the standard. Also, such systems don't use any feedback, so while they are much better than systems with primitive grammar, they still might have a hard time finding interesting examples. But we already have a big corpus of human-written interesting SQL queries &mdash; it's in our regression tests. Maybe we can use them as a base for fuzzing? We tried to do this, and it turned out to be surprisingly simple and efficient.

## AST-based Query Fuzzer

Consider some SQL query from a regression test. After parsing, it is easy to mutate the resulting AST (abstract syntax tree, an internal representation of the parsed query) before execution to introduce random changes into the query.  For strings and arrays, we make random modifications such as inserting a random character or doubling the string. For numbers, there are well-known Bad Numbers such as 0, 1, powers of two and nearby, integer limits, `NaN`. `NaN`s proved to be especially efficient in finding bugs, because you can often have some alternative branches in your numeric code, but for a `NaN`, both branches hold (or not) simultaneously, so this leads to nasty effects. 

Another interesting thing we can do is change the arguments of functions, or the list of expressions in `SELECT`, `ORDER BY` and so on. Naturally, all the interesting arguments can be taken from other test queries. Same goes for changing the tables used in the queries. When the fuzzer runs in CI, it runs queries from all the SQL tests in random order, mixing into them some parts of queries it has seen previously. This process can eventually cover all the possible permutations of our features.

The core implementation of the fuzzer is relatively small, consisting of about 700 lines of C++ code. A prototype was made in a couple of days, but naturally it took significantly longer to polish it and to start routinely using it in CI. It is very productive and let us find more than 200 bugs already (see the label [fuzz](https://github.com/ClickHouse/ClickHouse/labels/fuzz) on GitHub), some of which are serious logic errors or even memory errors. When we only started, we could segfault the server or make it enter a never-ending loop with simplest read-only queries such as `SELECT arrayReverseFill(x -> (x < 10), [])` or `SELECT geoDistance(0., 0., -inf, 1.)`. Of course I couldn't resist bringing down our [public playground](https://gh-api.clickhouse.tech/play?user=play#LS0gWW91IGNhbiBxdWVyeSB0aGUgR2l0SHViIGhpc3RvcnkgZGF0YSBoZXJlLiBTZWUgaHR0cHM6Ly9naC5jbGlja2hvdXNlLnRlY2gvZXhwbG9yZXIvIGZvciB0aGUgZGVzY3JpcHRpb24gYW5kIGV4YW1wbGUgcXVlcmllcy4Kc2VsZWN0ICdoZWxsbyB3b3JsZCc=) with some of these queries, and was content to see that the server soon restarts correctly.  These queries are actually minified by hand, normally the fuzzer would generate something barely intelligible such as:
```
SELECT
    (val + 257,
      (((tuple(NULL), 10.000100135803223), tuple(-inf)), '-1', (NULL, '0.10', NULL), NULL),
      (val + 9223372036854775807) = (rval * 100),
      tuple(65535), tuple(NULL), NULL, NULL),
    *
FROM 
(
    SELECT dummy AS val
    FROM system.one
) AS s1
ANY LEFT JOIN 
(
    SELECT toLowCardinality(toNullable(dummy)) AS rval
    FROM system.one
) AS s2 ON (val + 100) = (rval * 7)
```
In principle, we could add automated test case minification by modifying AST in the same vein with fuzzing. This is somewhat complicated by the fact that the server dies after every, excuse my pun, successfully failed query, so we didn't implement it yet.

Not all errors the fuzzer finds are significant, some of them are pretty boring and harmless, such as a wrong error code for an out-of-bounds argument. We still try to fix all of them, because this lets us ensure that under normal operation, the fuzzer doesn't find any errors.  This is similar to the approach usually taken with compiler warnings and other optional diagnostics &mdash; it's better to fix or disable every single case, so that you can be sure you have no diagnostics if everything is OK, and it's easy to notice new problems.

After fixing the majority of pre-existing error, this fuzzer became efficient for finding errors in new features. Pull requests introducing new features normally add an SQL test, and we pay extra attention to the new tests when fuzzing, generating more permutations for them. Even if the coverage of the test is not sufficient, there is a good chance that the fuzzer will find the missing corner cases. So when we see that all the fuzzer runs in different configurations have failed for a particular pull request, this almost always means that it introduces a new bug. When developing a feature that requires new grammar, it is also helpful to add fuzzing support for it. I did this for window functions early in the development, and it helped me find several bugs.

A major factor that makes fuzzing really efficient for us is that we have a lot of assertions and other checks of program logic in our code. For debug-only checks, we use the plain `assert` macro from `<cassert>`. For checks that are needed even in release mode, we use an exception with a special code `LOGICAL_ERROR` that signifies an internal program error. We did some work to ensure that these errors are distinct from errors caused by the wrong user actions. A user error reported for a randomly generated query is normal (e.g.  it references some non-existent columns), but when we see an internal program error, we know that it's definitely a bug, same as an assertion. Of course, even without assertions, you get some checks for memory errors provided by the OS (segfaults). Another way to add runtime checks to your program is to use some kind of sanitizer. We already run most of our tests under clang's Address, Memory, UndefinedBehavior and Thread sanitizers. Using them in conjunction with this fuzzer also proved to be very efficient.

To see for yourself how the fuzzer works, you only need the normal ClickHouse client.  Start `clickhouse-client --query-fuzzer-runs=100`, enter any query, and enjoy the client going crazy and running a hundred of random queries instead. All queries from the current session become a source for expressions for fuzzing, so try entering several different queries to get more interesting results. Be careful not to do this in production! When you do this experiment, you'll soon notice that the fuzzer tends to generate queries that take very long to run. This is why for the CI fuzzer runs we have to configure the server to limit query execution time, memory usage and so on using the corresponding [server settings](https://clickhouse.tech/docs/en/operations/settings/query-complexity/#:~:text=In%20the%20default%20configuration%20file,query%20within%20a%20single%20server.). We had a hilarious situation after that: the fuzzer figured out how to remove the limits by generating a `SET max_execution_time = 0` query, and then generated a never-ending query and failed. Thankfully we were able to defeat its cleverness by using [settings constraints](https://clickhouse.tech/docs/en/operations/settings/constraints-on-settings/).

## Other Fuzzers

The AST-based fuzzer we discussed is only one of the many kinds of fuzzers we have in ClickHouse. There is a [talk](https://www.youtube.com/watch?v=GbmK84ZwSeI&t=4481s) (in Russian, [slides are here](https://presentations.clickhouse.tech/cpp_siberia_2021/)) by Alexey Milovidov that explores all the fuzzers we have. Another interesting recent development is application of pivoted query synthesis technique, implemented in [SQLancer](https://github.com/sqlancer/sqlancer), to ClickHouse.  The authors are going to give [a talk about this](https://heisenbug-piter.ru/2021/spb/talks/nr1cwknssdodjkqgzsbvh/) soon, so stay tuned.

_2021-03-11 [Alexander Kuzmenkov](https://github.com/akuzm)_

