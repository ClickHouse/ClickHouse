import { createParser } from './parser.mjs';
import { instantiate } from './wasi-node.mjs';

export { FEATURE_FORMAT, FEATURE_DCL, FEATURE_AST_JSON } from './parser.mjs';

export const Parser = createParser({
    instantiate,
    wasmURL: new URL('../parser-no-formatting-no-dcl.wasm', import.meta.url),
});
