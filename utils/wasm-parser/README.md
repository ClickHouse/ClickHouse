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
| `ch_format(ptr, size, one_line)` | parse and format; 1 = ok, 0 = parse error |
| `ch_result_data()` / `ch_result_size()` | the formatted query, or the error message |

It runs on any engine: no flags, and in particular no WebAssembly exception-handling proposal.
A syntax error is not an exception - `tryParseQuery` returns null - and `src/Parsers` contains no
`catch` at all, which a style check enforces, so the build passes `-fignore-exceptions`. The
handful of parser checks that still report an invalid query by throwing - `Frame start cannot be
UNBOUNDED FOLLOWING`, for one - reach the same place through a `setjmp` boundary that
`wasm_runtime.cpp` jumps to instead of aborting. Recovery covers `DB::Exception` and nothing else:
anything else arriving there - a `std::bad_alloc` from `operator new`, say - is an object of an
unrelated type that cannot be read, so its type name is reported and the module stops.

## What it costs

`-Os`, stripped, all 220 parser translation units plus their transitive closure: **1.6 MB, 463 KB
gzipped**.

It started at 2.7 MB (789 KB gzipped), and almost none of the difference was parser code:
abseil (196 KB) and re2 (181 KB), the timezone database and cctz (30 KB plus tzdata), exception
landing pads and unwind tables (262 KB), `abi::__cxa_demangle` (132 KB), the per-error-code
statistics behind `system.errors` (150 KB of data), and `<locale>`, which twelve objects were
pulling in - 181 KB of it through a single `ostringstream` in Poco's `Bugcheck::what`.

The largest remaining components are `src/Parsers` itself, `src/Common` + `base`, then
`src/Parsers/Access` + `src/Access`, followed by fmt, double-conversion, zmij, Poco, `src/IO` and
`src/Core`. The entry point and the runtime shim are 11 KB together.

## What is left out, and why

`wasm_runtime.cpp` replaces a handful of chokepoints rather than building the real ones. Each is
something a browser has no use for, and each would otherwise dominate the bundle:

* **Stack traces.** `Common/Exception.cpp` cannot even compile here - it `static_assert`s on
  `STD_EXCEPTION_HAS_STACK_TRACE`, which comes from ClickHouse's patched libc++ in
  `contrib/libcxx-cmake`. WebAssembly cannot walk its own call stack from user code anyway.
* **Memory tracking and thread status.** Server bookkeeping; `malloc` is the only budget here.
* **The timezone database.** `contrib/cctz-cmake` generates `getTimeZone` with all of tzdata
  compiled into the binary. The parser no longer resolves timezones itself - `SYSTEM ... SET FAKE
  TIME` keeps the literal and the interpreter resolves it - but `SettingFieldTimezone` still names
  the entry point.
* **Query masking.** Configured on the server; there is nothing to mask client-side.
* **The exception machinery.** `__cxa_throw` and friends are defined here so that libc++abi's
  implementation stays out of the bundle entirely.

`Core/Settings.cpp` is no longer among them: `ParserSetQuery` used to call `Settings::castValueUtil`
to ask whether a bare `SET x` names a `Bool` setting, which pulled in every `SettingField*Traits`
specialization. It now records that the value was omitted and `BaseSettings::applyChange` checks
the type, where the schema is known - so the whole settings schema is out of the parser's closure,
in this build and in the server.

The `shim/` directory supplies the POSIX headers wasi-libc omits (`netdb.h`, `ucontext.h`,
`sched.h`, `pwd.h`, rounding modes in `fenv.h`, `sun_path`, `__u6_addr`). Nothing in the build
calls into them - they exist so that headers naming those types in signatures still compile.

## Where the remaining size is

Access-entity DDL is the largest single share: `CREATE USER`/`ROLE`/`QUOTA`/`ROW POLICY` and
`GRANT` cost 334 KB of parser and `Access` code directly. A "queries only" profile would be a
large, easy cut for a Web UI that never issues those statements.

What is *not* here any more is a regex engine. re2 and abseil used to cost 377 KB, reached from
`COLUMNS('regexp')` matchers, from `ASTFunction`'s secret-argument finder, and from access-rights
parsing. Applying a columns transformer needs the expanded column list, so it is name resolution
rather than parsing, and it moved to `Interpreters/applyColumnsTransformer.cpp`; the two other
sites were regular expressions doing what a string scan does.
