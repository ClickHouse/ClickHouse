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

`build.sh --no-formatting` builds a module that only answers whether a query parses. It has no
`ch_format`, and is 21% smaller - see below.

Nothing here needs the WebAssembly exception-handling proposal. `tryParseQuery` reports a syntax
error by returning null rather than by throwing, and no code in `src/Parsers` catches anything, so
the build passes `-fignore-exceptions` and a `throw` that does escape aborts.

## What it costs

Stripped, 320 translation units, `-Oz` with full LTO and `-fvirtual-function-elimination`:

| build | bytes | gzip -9 |
| --- | ---: | ---: |
| parse and format | 1168420 | 362693 |
| `--no-formatting` | 926077 | 297843 |

The first version of this build was 2.7 MB. Most of what went is listed under "What is left out";
the rest came from compiling for size rather than speed, from dropping locale support, and from
letting LTO remove the virtual functions nothing calls.

A per-component table is no longer meaningful: LTO inlines across translation units, so most of
the module cannot be attributed to any one source file.

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

That means it also has to stay out of parsing. Several parsers used to build a string by parsing a
fragment into an AST and formatting that AST back - `CAST(x AS T)` and `x::T` store `T` as a string
literal, `EPHEMERAL` stores the column type inside `defaultValueOfTypeName`, and
`(EXPLAIN ... SELECT ...)` becomes `viewExplain('<kind>', '<settings>', ...)`. They now take the
text straight from the query instead (`textBetween` in `Parsers/TokenIterator.h`), which is both
cheaper and closer to what the user wrote.

## Where the remaining size is

Roughly 30% of the bundle is access-entity DDL: `CREATE USER`/`ROLE`/`QUOTA`/`ROW POLICY` and
`GRANT` cost 334 KB of parser and `Access` code directly. A "queries only" profile would be a
large, easy cut for a Web UI that never issues those statements.

re2 and abseil (377 KB together) are pulled in by `COLUMNS('regexp')` matchers, by
`ASTFunction`'s secret-argument finder, and by access-rights parsing. `ASTColumnsRegexpMatcher`
compiles the pattern while *parsing*, which is what forces a regex engine into a component that
otherwise only builds a syntax tree.
