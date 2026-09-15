/// Node host for the WASI reactor. Same path `utils/wasm-parser/test.mjs` uses.
import { WASI } from 'node:wasi';

export async function instantiate(bytes)
{
    const wasi = new WASI({ version: 'preview1', args: [], env: {}, returnOnExit: true });
    const { instance } = await WebAssembly.instantiate(bytes, wasi.getImportObject());
    wasi.initialize(instance);
    return instance;
}
