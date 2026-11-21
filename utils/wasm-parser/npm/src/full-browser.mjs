import { createParser } from './parser.mjs';
import { instantiate } from './wasi-browser.mjs';

export const Parser = createParser({
    instantiate,
    wasmURL: new URL('../parser.wasm', import.meta.url),
});
