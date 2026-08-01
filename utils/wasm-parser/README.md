# Standalone WebAssembly build of the ClickHouse SQL parser

An experiment: how small can `src/Parsers` get when built on its own, for a browser?

The motivating use case is the Web UI (`programs/server/play.html`), which today only has the
lexer - `src/Parsers/Lexer.cpp` compiles to a 6 KB `.wasm` with no dependencies at all. A real
parser would additionally give exact syntax errors with positions, and query pretty-printing,
without a round trip to the server.

## Building

```bash
curl -sL https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-33/wasi-sdk-33.0-$(uname -m)-linux.tar.gz | tar xz -C tmp
./utils/wasm-parser/build.sh
node utils/wasm-parser/test.mjs tmp/wasm-parser/parser.wasm
```

The module is a WASI reactor and exports a C interface (see `wasm_parser.cpp`):

| export | meaning |
| --- | --- |
| `ch_alloc(size)` / `ch_free(ptr)` | allocate a buffer to write the query into |
| `ch_check(ptr, size)` | parse; 1 = ok, 0 = syntax error |
| `ch_format(ptr, size, one_line)` | parse and format; 1 = ok, 0 = parse error |
| `ch_result_data()` / `ch_result_size()` | the formatted query, or the error message |
| `ch_features()` | bit 0: `ch_format` is exported; bit 1: DCL parses |

`build.sh --no-formatting` builds a module that only answers whether a query parses; it has no
`ch_format`. `build.sh --no-dcl` builds one that does not accept access management. Both are
described below; `ch_features` reports which of them a given module was built with.

Nothing here needs the WebAssembly exception-handling proposal. `tryParseQuery` reports a syntax
error by returning null rather than by throwing, and no code in `src/Parsers` catches anything, so
the build passes `-fignore-exceptions`. The handful of parser checks that still report an invalid
query by throwing - `Frame start cannot be UNBOUNDED FOLLOWING`, for one - reach the same place
through a `setjmp` boundary that `wasm_runtime.cpp` jumps to instead of aborting. Recovery covers
`DB::Exception` and nothing else: anything else arriving there - a `std::bad_alloc` from
`operator new`, say - is an object of an unrelated type that cannot be read, so its type name is
reported and the module stops.

## What it costs

Stripped, 320 translation units, `-Oz` with full LTO and `-fvirtual-function-elimination`:

| build | bytes | gzip -9 | brotli -q 11 | zstd --ultra -22 |
| --- | ---: | ---: | ---: | ---: |
| everything | 1168636 | 362827 | 273666 | 293095 |
| `--no-dcl` | 966857 | 296331 | 225230 | 241372 |
| `--no-formatting` | 926511 | 298006 | 229139 | 246040 |
| `--no-formatting --no-dcl` | 754152 | 242329 | 188219 | 201737 |

Brotli is what a browser will actually get, and it is 24% better than gzip here.

The first version of this build was 2.7 MB. Most of what went is listed under "What is left out";
the rest came from compiling for size rather than speed, from dropping locale support, and from
letting LTO remove the virtual functions nothing calls.

A per-component table is no longer meaningful: LTO inlines across translation units, so most of
the module cannot be attributed to any one source file.

## How this compares

Sorted by brotli, which is what a browser negotiates:

| | bytes | gzip -9 | brotli -q 11 | zstd --ultra -22 |
| --- | ---: | ---: | ---: | ---: |
| [`node-sql-parser`](https://github.com/taozhi8833998/node-sql-parser) 5.4.0, one dialect, JS | 323347 | 71122 | 58777 | 62024 |
| [`@clickhouse/parser`](https://github.com/ClickHouse/clickhouse-js-parser) 0.3.0 + `zod`, JS | 1128583 | 196944 | 153347 | 161974 |
| [`libpg-query`](https://github.com/launchql/libpg-query-node) 17.7.4, wasm | 1150984 | 229158 | 168575 | 176785 |
| — its emscripten glue, on top of that | 58903 | 16679 | 14888 | 15718 |
| **this, `--no-formatting --no-dcl`** | 754152 | 242329 | 188219 | 201737 |
| **this, everything** | 1168636 | 362827 | 273666 | 293095 |
| [`sql.js`](https://github.com/sql-js/sql.js) 1.14.1, wasm | 659730 | 322193 | 278641 | 289690 |
| `node-sql-parser` 5.4.0, all 20+ dialects, JS | 2609025 | 504010 | 333174 | 360819 |
| [`@polyglot-sql/sdk`](https://github.com/tobilg/polyglot) 0.6.2, wasm | 21656938 | 4805067 | 2020675 | 2150089 |

The row to measure against is **`libpg-query`**: the same idea, a production database's own parser
compiled to WebAssembly rather than reimplemented. With its glue it is 183463 brotli against this
build's 188219 — the same ballpark, for a grammar of comparable size.

The two JS parsers are smaller, and both are reimplementations. `@clickhouse/parser` is a Peggy
grammar with Zod schemas for the same dialect, so it can drift from the server, where this cannot;
its published `index.mjs` is 3.6 MB unminified and imports `zod` externally, so the row above is
both, bundled and minified with esbuild, as a consumer would ship them. `node-sql-parser` is the
smallest thing here per dialect, and covers much less of any of them; loading all its dialects
costs more than this whole module.

`sql.js` is a whole SQLite — engine, storage and all, not a parser — and is here only for scale.
It is the one entry smaller than this uncompressed and larger after compression. `polyglot` is a
transpiler carrying thirty grammars and thirty generators; stripped, but built at Rust's default
release settings rather than for size.

`superjobru/clickhouse-sql-parser` is not in the table: an abandoned prototype, last commit
February 2022, about 55 KB of Rust handling basic `CREATE TABLE`, with nothing published to
measure.

## What is left out, and why

`wasm_runtime.cpp` replaces a handful of chokepoints rather than building the real ones. Each is
something a browser has no use for, and each would otherwise dominate the bundle:

* **Stack traces.** `Common/Exception.cpp` cannot even compile here - it `static_assert`s on
  `STD_EXCEPTION_HAS_STACK_TRACE`, which comes from ClickHouse's patched libc++ in
  `contrib/libcxx-cmake`. WebAssembly cannot walk its own call stack from user code anyway.
* **Memory tracking and thread status.** Server bookkeeping; `malloc` is the only budget here.
* **`Core/Settings.cpp`.** `ParserSetQuery` calls `Settings::castValueUtil` once, only to ask
  whether a bare `SET x` names a `Bool` setting. That single call pulls in the whole settings
  schema - every `SettingField*Traits` specialization - which is larger than the parser itself.
* **The timezone database.** `contrib/cctz-cmake` generates `getTimeZone` with all of tzdata
  compiled into the binary.
* **Query masking.** Configured on the server; there is nothing to mask client-side.

The `shim/` directory supplies the POSIX headers wasi-libc omits (`netdb.h`, `ucontext.h`,
`sched.h`, `pwd.h`, rounding modes in `fenv.h`, `sun_path`, `__u6_addr`). Nothing in the build
calls into them - they exist so that headers naming those types in signatures still compile.

## Formatting is all or nothing

Turning an AST back into SQL costs 242 KB, a fifth of the module, and none of it can be dropped
piecemeal. `formatImpl` is virtual, every AST class overrides it, and the linker has to keep all
116 implementations as long as one call goes through that slot - so the only way to leave it out
is to leave out the last call, which is what `-DCLICKHOUSE_PARSER_NO_FORMATTING` does.

The awkward part is that parsing itself formats, in the few places that have to keep a fragment of
the query as a string rather than as a subtree: `CAST(x AS T)` and `x::T` store `T` as a string
literal, `EPHEMERAL` stores the column type inside `defaultValueOfTypeName`, and
`(EXPLAIN ... SELECT ...)` becomes `viewExplain('<kind>', '<settings>', ...)`. All four go through
`astText`, which formats normally and uses the query text - `textBetween` in
`Parsers/TokenIterator.h` - when there is no formatter.

The query text is not a general substitute. It is what the user wrote, line breaks and all, and the
formatted spelling is the canonical one that ends up in table metadata:

```sql
CREATE TABLE t (x UInt8, e Enum8
    (
        'hello' = 1
    ) DEFAULT CAST(x AS Enum8('hello' = 1)))
```

stores `CAST(x, 'Enum8(\'hello\' = 1)')` today, and would store the newlines and the indentation if
the parser kept the source text - `SHOW CREATE TABLE` would then print them back. A build that only
answers whether a query parses never looks at the string, so there it does not matter.

## What each kind of query costs

`--no-dcl` leaves out access management - `CREATE USER`, `CREATE ROLE`, quotas, row policies,
settings profiles, masking policies, `GRANT`, `REVOKE`, `CHECK GRANT`, `SET ROLE`, `EXECUTE AS`,
`SHOW GRANTS`, `SHOW ACCESS`, `SHOW CREATE USER` and `SHOW PRIVILEGES`. `CLICKHOUSE_PARSER_NO_DCL`
takes them out of the two dispatch functions in `ParserQuery.cpp` and `ParserQueryWithOutput.cpp`,
and the linker then drops `src/Parsers/Access` and `src/Access/Common` with them. `DEFINER` on a
view still parses: it is part of `CREATE`, not of access management.

It is worth a build option because nothing else comes close. Below is what each family of
statements costs at the margin - the whole module, minus that one family:

| left out | bytes | gzip -9 |
| --- | ---: | ---: |
| DCL (`--no-dcl`) | 201779 | 66496 |
| `CREATE TABLE` / `VIEW` / `DATABASE` | 56533 | 16731 |
| functions, workloads, resources, named collections, indexes | 45644 | 13070 |
| `ALTER` | 35978 | 7857 |
| `SYSTEM` | 32086 | 10810 |
| `KILL`, `WATCH`, `CHECK`, `OPTIMIZE`, `RENAME`, `DROP`, `UNDROP` | 23858 | 6987 |
| `BACKUP` / `RESTORE` | 17431 | 5772 |
| `DESCRIBE`, `EXISTS`, table properties | 13356 | 2412 |
| `SHOW` (other than the access ones) | 12803 | 3231 |
| `INSERT` | 11611 | 3734 |
| `DELETE`, `UPDATE`, `COPY`, transactions | 9254 | 2318 |
| `USE`, `SET` | 1352 | 425 |

DCL is 3.6x the next entry, and costs about as much as the five below it together. The reason is that it
is not only grammar: `GRANT` needs the privilege lattice in `src/Access/Common` - `AccessFlags`,
`AccessRightsElement`, the full list of privileges and their hierarchy - and `CREATE USER` needs
authentication types, host patterns and IP subnets.

The rest do not merit an option each. They are grammar and AST nodes, they overlap heavily, and
the numbers do not add up the way the table might suggest: `ALTER` and `CREATE` share the column
and index declarations, everything shares expressions, and no combination of them removes the
shared core. Measure any combination before promising it - each row was measured on its own,
against the full build.

Three of these were misleading at first. `SYSTEM`, `INSERT` and `CREATE` are also instantiated by
`ParserExplainQuery`, so taking them out of `ParserQuery` alone saves almost nothing - `SYSTEM`
appeared to cost 41 bytes. The table is from a build that removes them from both.

## Where the remaining size is

re2 and abseil are pulled in by `COLUMNS('regexp')` matchers and by `ASTFunction`'s
secret-argument finder. `ASTColumnsRegexpMatcher` compiles the pattern while *parsing*, which is
what forces a regex engine into a component that otherwise only builds a syntax tree.
