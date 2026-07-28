# Standalone WebAssembly build of the ClickHouse SQL parser

An experiment: how small can `src/Parsers` get when built on its own, for a browser?

The motivating use case is the Web UI (`programs/server/play.html`), which today only has the
lexer - `src/Parsers/Lexer.cpp` compiles to a 6 KB `.wasm` with no dependencies at all. A real
parser would additionally give exact syntax errors with positions, and query pretty-printing,
without a round trip to the server.

## Building

```bash
# wasi-sdk names the AArch64 build `arm64`, not `aarch64`
curl -sL https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-33/wasi-sdk-33.0-$(uname -m | sed s/aarch64/arm64/)-linux.tar.gz | tar xz -C tmp
./utils/wasm-parser/build.sh
node utils/wasm-parser/test.mjs tmp/wasm-parser/parser.wasm
```

The module is a WASI reactor and exports a C interface (see `wasm_parser.cpp`):

| export | meaning |
| --- | --- |
| `ch_alloc(size)` / `ch_free(ptr)` | allocate a buffer to write the query into |
| `ch_format(ptr, size, one_line)` | parse and format; 1 = ok, 0 = parse error |
| `ch_result_data()` / `ch_result_size()` | the formatted query, or the error message |

It runs on a plain engine: the build is compiled with `-fignore-exceptions`, so no unwinding is
emitted and the WebAssembly exception-handling proposal is not needed. A parser that does not
match returns `false` rather than throwing (see check 19 in
`ci/jobs/scripts/check_style/check_cpp.sh`), and a syntax error is reported through `ch_format`
returning 0. The handful of parser checks that still report an invalid query by throwing - `Frame
start cannot be UNBOUNDED FOLLOWING`, for one - reach the same place through a `setjmp` boundary
that `wasm_runtime.cpp` jumps to instead of aborting. Recovery covers `DB::Exception` and nothing
else: anything else arriving there - a `std::bad_alloc` from `operator new`, say - is an object of
an unrelated type that cannot be read, so its type name is reported and the module stops.

## What it costs

`-Os`, stripped, all 320 translation units of the parser and its transitive closure. Sizes are
attributed to the defining object with the linker map:

| component | KB | share |
| --- | ---: | ---: |
| `src/Parsers` | 884 | 45% |
| `src/Common` + `base` | 334 | 17% |
| `src/Parsers/Access` | 148 | 7% |
| `src/Access` | 142 | 7% |
| fmt, double-conversion | 86 | 4% |
| `src/IO` + `src/Core` | 40 | 2% |
| Poco | 37 | 2% |
| entry point + runtime shim | 3 | 0% |

2.0 MB total, 573 KB gzipped. The 290 KB unaccounted for above are the data segments, the wasm
type/import/export sections and wasi-libc.

## What is left out, and why

`wasm_runtime.cpp` replaces a handful of chokepoints rather than building the real ones. Each is
something a browser has no use for, and each would otherwise dominate the bundle:

* **Stack traces.** `Common/Exception.cpp` cannot even compile here - it `static_assert`s on
  `STD_EXCEPTION_HAS_STACK_TRACE`, which comes from ClickHouse's patched libc++ in
  `contrib/libcxx-cmake`. WebAssembly cannot walk its own call stack from user code anyway.
* **Memory tracking and thread status.** Server bookkeeping; `malloc` is the only budget here.
* **Query masking.** Configured on the server; there is nothing to mask client-side.

`Core/Settings.cpp` and the timezone database used to be on this list. They are not left out any
more - the parser no longer reaches them at all. `ParserSetQuery` used to call
`Settings::castValueUtil`, only to ask whether a bare `SET x` names a `Bool` setting; it now
records that the value was omitted and `BaseSettings::applyChange` checks the type, where the
schema is known. `SYSTEM ... SET FAKE TIME` used to parse and format its argument through
`DateLUT`, which compiled all of tzdata into the module; the AST keeps the literal text and the
interpreter resolves it against the server timezone, which is where that decision belongs.

The `shim/` directory supplies the POSIX headers wasi-libc omits (`netdb.h`, `ucontext.h`,
`sched.h`, `pwd.h`, rounding modes in `fenv.h`, `sun_path`, `__u6_addr`). Nothing in the build
calls into them - they exist so that headers naming those types in signatures still compile.

## Where the remaining size is

Access-entity DDL is the largest single candidate for removal: `CREATE USER`/`ROLE`/`QUOTA`/`ROW
POLICY` and `GRANT` cost 290 KB of parser and `Access` code directly, 15% of the module. A
"queries only" profile would be a large, easy cut for a Web UI that never issues those
statements.

re2, abseil and cctz used to account for another 407 KB. They are gone: `COLUMNS('regexp')` and
`GRANT ... ON S3('...')` patterns are compiled where the pattern is *used* rather than where it
is parsed (`Interpreters/applyColumnsTransformer.cpp` and `InterpreterGrantQuery`), the two
secret-masking regular expressions became string scans, and `IParser.h` and `LiteralTokenMap`
have their own containers.
