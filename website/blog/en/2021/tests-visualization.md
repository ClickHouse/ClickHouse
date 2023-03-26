---
title: 'Decorating a Christmas Tree With the Help Of Flaky Tests'
image: 'https://blog-images.clickhouse.com/en/2021/tests-visualization/tests.png'
date: '2021-12-27'
author: 'Alexey Milovidov'
tags: ['tests', 'ci', 'flaky', 'christmas', 'visualization']
---

Test suites and testing infrastructure are one of the main assets of ClickHouse. We have tons of functional, integration, unit, performance, stress and fuzz tests. Tests are run on a per commit basis and results are publicly available.

We also save the results of all test runs into the database in ClickHouse. We started collecting results in June 2020, and we have 1 777 608 240 records so far. Now we run around 5 to 9 million tests every day.

Tests are good (in general). A good test suite allows for fast development iterations, stable releases, and to accept more contributions from the community. We love tests. If there's something strange in ClickHouse, what are we gonna do? Write more tests.

Some tests can be flaky. The reasons for flakiness are uncountable - most of them are simple timing issues in the test script itself, but sometimes if a test has failed one of a thousand times it can uncover subtle logic errors in code.

The problem is how to deal with flaky tests. Some people suggest automatically muting the "annoying" flaky tests. Or adding automatic retries in case of failure. We believe that this is all wrong. Instead of trying to ignore flaky tests, we do the opposite: we put maximum effort into making the tests even more flaky!

Our recipes for flaky tests:
— never mute or restart them; if the test failed once, always look and investigate the cause;
— randomize the environment for every test run so the test will have more possible reasons to fail;
— if new tests are added, run them 100 times and if at least one fails, do not merge the pull request;
— if new tests are added, use them as a corpus for fuzzing - it will uncover corner cases even if author did not write tests for them;
— [randomize thread scheduling](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ThreadFuzzer.h) and add random sleeps and switching between CPU cores at random places and before and after mutex locks/unlocks;
— run everything in parallel on slow machines;

Key point: to prevent flaky tests, we make our tests as flaky as possible.

## Nice Way To Visualize Flaky Tests

There is a test suite named "[functional stateless tests](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless)" that has 3772 tests. For every day since 2020-06-13 (561 days) and every test (3772 tests), I drew a picture of size 561x3772 where a pixel is green if all test runs finished successfully in the master branch during this day (for all commits and all combinations: release, debug+assertions, ASan, MSan, TSan, UBSan), and a pixel is red if at least one run failed. The pixel will be transparent if the test did not exist that day.

This visualization is a toy that I've made for fun:

![Visualization](https://blog-images.clickhouse.com/en/2021/tests-visualization/tree_half.png)

It looks like a Christmas Tree (you need a bit of imagination). If you have a different kind of imagination, you can see it as a green field with flowers.

The time is from left to right. The tests are numbered with non-unique numbers (new tests usually get larger numbers), and these numbers are on the vertical axis (newer tests on top).

If you see red dots in a horizontal line - it is a flaky test. If you see red dots in a vertical line - it means that one day we accidentally broke the master branch. If you see black horizontal lines or cuts in the tree - it means that the tests were added with some old numbers, most likely because some long living feature branch was merged. If you see black vertical lines - it means that some days tests were not run.

The velocity of adding new tests is represented by how tall and narrow the Christmas tree is. When we add a large number of tests, the tree grows with almost vertical slope.

The image is prepared by [HTML page](https://github.com/ClickHouse/ClickHouse/pull/33185) with some JavaScript that is querying a ClickHouse database directly and writing to a canvas. It took around ten seconds to build this picture. I also prepared an [interactive version](https://blog-images.clickhouse.com/en/2021/tests-visualization/demo.html) with already-saved data where you can play and find your favorite tests.
