## This parser is unsupported

We keep it in this repository for your curiosity. But this is not the parser of ClickHouse.

## How to generate source code files from grammar

Grammar is located inside `ClickHouseLexer.g4` and `ClickHouseParser.g4` files.

To generate source code you need to install locally the `antlr4` binary. The grammar works with every ANTLR target. For example, for C++:
```
cd utils/antlr
antlr4 -no-listener -visitor -package DB -Dlanguage=Cpp ClickHouseLexer.g4 ClickHouseParser.g4
```

Change `-Dlanguage` for another target, such as `Java`, `Python3`, `JavaScript` or `Go`. The generated files are not stored in the repository.

## Notes on the grammar

The grammar has no actions and no semantic predicates, so it works with every ANTLR target.
Where ClickHouse accepts a clause at most once, the grammar accepts it any number of times, in any order. This applies to:
- the attributes of a dictionary column and the clauses after the columns of `CREATE DICTIONARY`,
- the clauses after the table engine, such as `ORDER BY`, `PARTITION BY` and `SETTINGS`,
- the modifiers of a column, such as `DEFAULT`, `CODEC` and `TTL`,
- the `VALID` and `GRANTS` clauses of `CREATE TOKEN`,
- `ON CLUSTER` in `GRANT` and `REVOKE`.

`GRANT` and `REVOKE` read the privileges as a list of words, as ClickHouse does, so the grammar does not check that a privilege exists.

The grammar covers only a part of the ClickHouse syntax. When you add a rule, check it against `src/Parsers`.
