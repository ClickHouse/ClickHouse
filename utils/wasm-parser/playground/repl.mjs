/// Local REPL against `@clickhouse/wasm-parser` via `file:../npm`.
/// Run `npm run build` in `../npm` first so `parser.wasm` exists, then here: `npm install && npm start`.
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { start } from 'node:repl';
import { fileURLToPath } from 'node:url';
import { Parser } from '@clickhouse/wasm-parser';
import { Parser as SlimParser } from '@clickhouse/wasm-parser/slim';

const wasm = join(dirname(fileURLToPath(import.meta.url)), '../npm/parser.wasm');
if (!existsSync(wasm))
{
    process.stderr.write(
        'missing ../npm/parser.wasm; run `npm run build` in utils/wasm-parser/npm first\n');
    process.exit(1);
}

await Parser.init();
await SlimParser.init();

process.stdout.write(
    'Parser is ready (also SlimParser).\n'
    + '  Parser.parse(\'SELECT 1\')\n'
    + '  Parser.format(\'select 1\', { oneLine: true })\n'
    + '  Parser.formatJson(Parser.parse(\'SELECT 1\').ast, { oneLine: true })\n'
    + '  Parser.features\n\n');

const server = start({ prompt: 'wasm-parser> ' });
server.context.Parser = Parser;
server.context.SlimParser = SlimParser;
