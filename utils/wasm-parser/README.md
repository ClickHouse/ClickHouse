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
node --experimental-wasm-exnref utils/wasm-parser/test.mjs tmp/wasm-parser/parser.wasm
```

The module is a WASI reactor and exports a C interface (see `wasm_parser.cpp`):

| export | meaning |
| --- | --- |
| `ch_alloc(size)` / `ch_free(ptr)` | allocate a buffer to write the query into |
| `ch_format(ptr, size, one_line)` | parse and format; 1 = ok, 0 = parse error |
| `ch_result_data()` / `ch_result_size()` | the formatted query, or the error message |

It needs an engine with the WebAssembly exception-handling proposal. `-fwasm-exceptions` is not
optional: the parser reports errors by throwing, and the wasi-sdk headers refuse `setjmp.h`
without it.

## What it costs

`-Os`, stripped, all 220 parser translation units plus their transitive closure:

| component | KB | share |
| --- | ---: | ---: |
| `src/Parsers` | 1063 | 45% |
| `src/Common` + `base` | 356 | 15% |
| abseil | 196 | 8% |
| re2 | 181 | 8% |
| `src/Parsers/Access` | 178 | 7% |
| `src/Access` | 156 | 7% |
| fmt, double-conversion, zmij | 94 | 4% |
| Poco | 63 | 3% |
| `src/IO` + `src/Core` | 46 | 2% |
| cctz | 30 | 1% |
| entry point + runtime shim | 11 | 0% |

2.7 MB total, 789 KB gzipped.

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

## Where the remaining size is

Roughly 30% of the bundle is access-entity DDL: `CREATE USER`/`ROLE`/`QUOTA`/`ROW POLICY` and
`GRANT` cost 334 KB of parser and `Access` code directly. A "queries only" profile would be a
large, easy cut for a Web UI that never issues those statements.

re2 and abseil (377 KB together) are pulled in by `COLUMNS('regexp')` matchers, by
`ASTFunction`'s secret-argument finder, and by access-rights parsing. `ASTColumnsRegexpMatcher`
compiles the pattern while *parsing*, which is what forces a regex engine into a component that
otherwise only builds a syntax tree.
