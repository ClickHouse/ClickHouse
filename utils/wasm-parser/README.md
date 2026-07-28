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
| `ch_error_data()` / `ch_error_size()` | the message of the exception that made the module trap |

It runs on any engine: no flags, and in particular no WebAssembly exception-handling proposal.
The build uses `-fignore-exceptions`: nothing can be caught, and `src/Parsers` contains no `catch`
at all, which a style check enforces.

A syntax error is not an exception - `tryParseQuery` returns null and `ch_format` returns 0 - but
an exception is not confined to bugs either. `IParser::Pos` throws `TOO_DEEP_RECURSION` and
`TOO_SLOW` when a query exceeds the depth or backtracking limit that `ch_format` passes, and a few
parsers throw on input they have already committed to, so ordinary input can reach a `throw`. There
is nothing to unwind to, so `__cxa_throw` records the message and traps. A trap returns control to
the embedder and leaves linear memory intact, so the contract for a caller is:

* if the call traps, read `ch_error_data`/`ch_error_size` for the message, then throw the instance
  away and instantiate the module again - the module is compiled once and instantiating is cheap,
  but the allocator was interrupted mid-operation, so the instance must not be reused.

Making those queries return an error instead of trapping needs real unwinding, i.e. building with
`-fwasm-exceptions` and catching in `ch_format`. That costs bundle size and requires the engine to
implement the exception-handling proposal, which is why this build does not do it.

## What it costs

`-Oz`, full LTO with `-fvirtual-function-elimination`, stripped, all 320 translation units of the
parser and its transitive closure: **1.17 MB, 362 KB gzipped, 273 KB brotli**.

It started at 2.7 MB (789 KB gzipped), and almost none of the difference was parser code:
abseil (196 KB) and re2 (181 KB), the timezone database and cctz (30 KB plus tzdata), exception
landing pads and unwind tables (262 KB), `abi::__cxa_demangle` (132 KB), the per-error-code
statistics behind `system.errors` (150 KB of data), and `<locale>`, which twelve objects were
pulling in - 181 KB of it through a single `ostringstream` in Poco's `Bugcheck::what`. `-Oz` over
`-Os` accounts for a further 19%, at the cost of about 22% of parse throughput - the right way
round for something downloaded once and then asked to parse one query at a time.

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
specialization. It now records that the value was omitted, and the layers that apply a
`SettingChange` - `BaseSettings::applyChange`, `Context::applySettingChange` and
`Context::checkSettingsConstraints` - check the type where the schema is known. So the whole
settings schema is out of the parser's closure, in this build and in the server.

The `shim/` directory supplies the POSIX headers wasi-libc omits (`netdb.h`, `ucontext.h`,
`sched.h`, `pwd.h`, rounding modes in `fenv.h`, `sun_path`, `__u6_addr`). Nothing in the build
calls into them - they exist so that headers naming those types in signatures still compile.

## Where the remaining size is

Access-entity DDL is the largest single share: `CREATE USER`/`ROLE`/`QUOTA`/`ROW POLICY` and
`GRANT` cost 334 KB of parser and `Access` code directly, as measured before `-Oz` and LTO. A
"queries only" profile would be a large, easy cut for a Web UI that never issues those statements.

What is *not* here any more is a regex engine. re2 and abseil used to cost 377 KB, reached from
`COLUMNS('regexp')` matchers, from `ASTFunction`'s secret-argument finder, and from access-rights
parsing. Applying a columns transformer needs the expanded column list, so it is name resolution
rather than parsing, and it moved to `Interpreters/applyColumnsTransformer.cpp`; the two other
sites were regular expressions doing what a string scan does.
