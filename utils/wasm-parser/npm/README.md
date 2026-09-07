# `@clickhouse/wasm-parser`

Experimental JavaScript wrapper around the standalone ClickHouse SQL parser
WebAssembly module. The JS API is unstable. This package is **not published to
the npm registry**; CI uploads an `npm pack` tarball next to `parser.wasm`.

The C ABI is unchanged. This directory is packaging only.

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
Parser.features

const { ast, highlights, error } = Parser.parse(sql)
const { sql, error } = Parser.format(query, { oneLine: true })
const { sql, error } = Parser.formatJson(ast, { oneLine: true })
```

`parse` / `format` / `formatJson` are synchronous after `init`. Calling them
before `await Parser.init()` throws. SQL failures never throw: they set
`error`.

The slim build is `@clickhouse/wasm-parser/slim`. `format` and `formatJson`
return `{ error: { message: 'format is not in this build' } }`.

`init` is idempotent. The default wasm URL is the file packed next to `src/`
(`import.meta.url`). In a bundler or worker, pass `{ url }` or `{ bytes }`.

## Pack locally

From a tree that already has the two wasm artifacts (see the parent
`utils/wasm-parser/README.md` for the CMake build):

```bash
node utils/wasm-parser/npm/scripts/pack.mjs \
  --wasm-dir <dir-with-both-wasm-files> \
  --out-dir tmp
```
