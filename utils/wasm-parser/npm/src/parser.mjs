/// Shared `Parser` object: C ABI in `wasm_parser.cpp`, driven without Emscripten glue.
/// `instantiate` comes from `wasi-node.mjs` or `wasi-browser.mjs`. The `.wasm` is a sibling of
/// `src/` at pack time (`parser.wasm` / `parser-no-formatting-no-dcl.wasm`).

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const NOT_INIT =
    'Parser.init() was not called; await Parser.init() first';

const NO_FORMAT = 'format is not in this build';

export function createParser({ instantiate, wasmURL })
{
    let exports_ = null;
    let initPromise = null;

    function requireReady()
    {
        if (exports_ === null)
            throw new TypeError(NOT_INIT);
        return exports_;
    }

    function isFileUrl(url)
    {
        if (url instanceof URL)
            return url.protocol === 'file:';
        return typeof url === 'string' && url.startsWith('file:');
    }

    async function loadWasm(url)
    {
        if (isFileUrl(url))
        {
            const { readFile } = await import('node:fs/promises');
            const { fileURLToPath } = await import('node:url');
            const href = url instanceof URL ? url.href : url;
            return readFile(fileURLToPath(href));
        }
        const href = url instanceof URL ? url.href : url;
        const response = await fetch(href);
        if (!response.ok)
            throw new Error(`failed to load wasm: ${response.status} ${href}`);
        return new Uint8Array(await response.arrayBuffer());
    }

    function call(entry, input)
    {
        const { memory, ch_alloc, ch_free, ch_result_data, ch_result_size } = requireReady();
        const bytes = encoder.encode(input);
        const ptr = ch_alloc(bytes.length);
        if (!ptr)
            throw new Error('ch_alloc returned null');
        new Uint8Array(memory.buffer, ptr, bytes.length).set(bytes);
        try
        {
            const ok = entry(ptr, bytes.length);
            const out = decoder.decode(
                new Uint8Array(memory.buffer, ch_result_data(), ch_result_size()).slice());
            return { ok: !!ok, out };
        }
        finally
        {
            ch_free(ptr);
        }
    }

    const Parser = {
        async init(options = {})
        {
            if (initPromise)
                return initPromise;

            initPromise = (async () =>
            {
                let bytes = options.bytes;
                if (bytes === undefined)
                    bytes = await loadWasm(options.url ?? wasmURL);
                if (bytes instanceof ArrayBuffer)
                    bytes = new Uint8Array(bytes);
                const instance = await instantiate(bytes);
                exports_ = instance.exports;
            })();

            try
            {
                await initPromise;
            }
            catch (error)
            {
                initPromise = null;
                throw error;
            }
        },

        get features()
        {
            const mask = requireReady().ch_features();
            return {
                format: (mask & 1) !== 0,
                dcl: (mask & 2) !== 0,
                astJson: (mask & 4) !== 0,
            };
        },

        parse(sql)
        {
            const result = call(requireReady().ch_parse, sql);
            try
            {
                return JSON.parse(result.out);
            }
            catch
            {
                return { error: { message: result.out } };
            }
        },

        format(sql, options = {})
        {
            const ch_format = requireReady().ch_format;
            if (typeof ch_format !== 'function')
                return { error: { message: NO_FORMAT } };
            const result = call((ptr, len) => ch_format(ptr, len, options.oneLine ? 1 : 0), sql);
            if (result.ok)
                return { sql: result.out };
            return { error: { message: result.out } };
        },

        formatJson(ast, options = {})
        {
            const ch_format_json = requireReady().ch_format_json;
            if (typeof ch_format_json !== 'function')
                return { error: { message: NO_FORMAT } };
            const input = typeof ast === 'string' ? ast : JSON.stringify(ast);
            const result = call(
                (ptr, len) => ch_format_json(ptr, len, options.oneLine ? 1 : 0),
                input);
            if (result.ok)
                return { sql: result.out };
            return { error: { message: result.out } };
        },
    };

    return Parser;
}
