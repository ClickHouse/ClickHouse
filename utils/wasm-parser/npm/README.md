# `@clickhouse/wasm-parser`

Experimental JavaScript wrapper around the standalone ClickHouse SQL parser
WebAssembly module. The JS API is unstable. This package is **not published to
the npm registry**; CI uploads an `npm pack` tarball next to `parser.wasm`.

The C ABI is unchanged. `scripts/` is local DX for the CMake build; the published
tarball is still packaging only.

## Install from CI

After `Build (wasm_parser)` on a commit:

```json
{
  "dependencies": {
    "@clickhouse/wasm-parser": "https://clickhouse-builds.s3.amazonaws.com/REFs/master/<sha>/build_wasm_parser/clickhouse-wasm-parser.tgz"
  }
}
```

`<version>` is `MAJOR.MINOR.PATCH-dev.<sha7>` inside `package.json` of that tarball
(for example `26.9.1-dev.94a1553`). Pull-request artifacts use `PRs/<pr>/<sha>/`
instead of `REFs/master/<sha>/`.

The tarball includes `parser.wasm` (formatting, DCL, AST JSON) and
`parser-no-formatting-no-dcl.wasm`.

## API

Requires a host with the WebAssembly exception-handling proposal (Chrome 95,
Firefox 100, Safari 18.2, Node 22 as used by `clickhouse/wasm-builder`).

```js
import { Parser } from '@clickhouse/wasm-parser'

await Parser.init() // or { url } / { bytes } for a bundler asset
Parser.features // { format: true, dcl: true, astJson: true }

const { ast, highlights, error } = Parser.parse(sql)
const { sql, error } = Parser.format(query, { oneLine: true })
const { sql, error } = Parser.formatJson(ast, { oneLine: true })
```

`parse` / `format` / `formatJson` are synchronous after `init`. Calling them
before `await Parser.init` throws. SQL failures never throw: they set
`error`.

`highlights` and parse-error `begin` / `end` are UTF-8 byte offsets (end
exclusive), not JavaScript string indices, so non-ASCII SQL will not line up
with `String.prototype.slice`.

The slim build is `@clickhouse/wasm-parser/slim`. `format` and `formatJson`
return `{ error: { message: 'format is not in this build' } }`.

`init` is idempotent. The default wasm URL is the file packed next to `src/`
(`import.meta.url`). In a bundler or worker, pass `{ url }` or `{ bytes }`.
Scheme-less paths such as `/assets/parser.wasm` are fetched; filesystem paths
must be `file:` URLs.

## Build locally

The `.wasm` modules and the npm tarball are generated; they are not committed.

You need Node.js 22+, `cmake` (>= 3.24), `ninja`, `git`, and `curl`. `setup`
does not install those — only wasi-sdk 33 for this OS/arch (Linux or macOS,
x64 or arm64).

```bash
cd utils/wasm-parser/npm
npm run setup
npm run build
npm test
```

`setup` downloads wasi-sdk into `<repo>/tmp/wasi-sdk` (or uses `WASI_SDK` if
that already points at a usable prefix). `build` compiles both wasm
configurations, copies `parser.wasm` and `parser-no-formatting-no-dcl.wasm`
next to this package, and writes `tmp/wasm-parser/clickhouse-wasm-parser.tgz`.

If you already have the two wasm artifacts, pack without rebuilding:

```bash
node utils/wasm-parser/npm/scripts/pack.mjs \
  --wasm-dir <dir-with-both-wasm-files> \
  --out-dir tmp
```

## Interactive playground

An interactive Node.js REPL is available in `utils/wasm-parser/playground` to experiment with `@clickhouse/wasm-parser` in a terminal.

### Starting the playground

Compile the `.wasm` modules first if they are not already built, then start the REPL:

```bash
cd utils/wasm-parser/npm
npm run setup
npm run build

cd ../playground
npm install
npm start
```

Both `Parser` (full build) and `SlimParser` (slim build) are pre-initialized in the global context.

### What is possible

- **Parse SQL and inspect the AST tree**:
  ```js
  const res = Parser.parse('SELECT number * 2 AS val FROM numbers(10) WHERE val > 5')
  console.dir(res.ast, { depth: null })
  ```
- **Syntax token highlighting offsets**:
  ```js
  Parser.parse('SELECT 1').highlights
  // [ { begin: 0, end: 6, type: 'keyword' }, { begin: 7, end: 8, type: 'number' } ]
  ```
- **Parse error diagnostics without exceptions**:
  ```js
  const res = Parser.parse('SELECT * FORM table')
  console.log(res.error)
  // { message: "Syntax error: failed at position 10 ('FORM') ...", expected: [ ... ] }
  ```
- **SQL formatting and query normalization**:
  ```js
  Parser.format('select   1 as a,   2 as b', { oneLine: true })
  // { sql: 'SELECT 1 AS a, 2 AS b' }
  ```
- **Round-trip and transform AST JSON**:
  ```js
  const { ast } = Parser.parse('SELECT 1')
  Parser.formatJson(ast, { oneLine: true })
  // { sql: 'SELECT 1' }
  ```
- **Compare full vs slim parser capabilities**:
  ```js
  Parser.features     // { format: true, dcl: true, astJson: true }
  SlimParser.features // { format: false, dcl: false, astJson: false }

  SlimParser.parse('SELECT 1')   // syntax validation succeeds
  SlimParser.format('SELECT 1')  // { error: { message: 'format is not in this build' } }
  ```

