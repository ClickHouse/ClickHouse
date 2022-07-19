How to use Fuzzer
===

The fuzzer consists of auto-generated files:

    ClickHouseUnlexer.py
    ClickHouseUnparser.py

They are generated from grammar files (.g4) using Grammarinator:

    pip3 install grammarinator
    grammarinator-process ClickHouseLexer.g4 ClickHouseParser.g4 -o fuzzer/

Then you can generate test input for ClickHouse client:

    cd fuzzer
    grammarinator-generate \
      -r query_list                                               \ # top-level rule
      -o /tmp/sql_test_%d.sql                                     \ # template for output test names
      -n 10                                                       \ # number of tests
      -c 0.3                                                      \
      -d 20                                                       \ # depth of recursion
      -p ClickHouseUnparser.py -l ClickHouseUnlexer.py            \ # auto-generated unparser and unlexer
      --test-transformers SpaceTransformer.single_line_whitespace \ # transform function to insert whitespace

For more details see `grammarinator-generate --help`. As a test-transformer function also can be used `SpaceTransformer.multi_line_transformer` - both functions reside in `fuzzer/SpaceTransformer.py` file.


Parsing steps
===

1. Replace all operators with corresponding functions.
2. Replace all asterisks with columns - if it's inside function call, then expand it as multiple arguments. Warn about undeterministic invocations when functions have positional arguments.

Old vs. new parser
===

- `a as b [c]` - accessing aliased array expression is not possible.
- `a as b . 1` - accessing aliased tuple expression is not possible.
- `between a is not null and b` - `between` operator should have lower priority than `is null`.
- `*.1` - accessing asterisk tuple expression is not possible.
