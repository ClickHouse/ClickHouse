// Examples for the AssemblyScript ABI (ASSEMBLYSCRIPT_V1) of ClickHouse WASM UDFs.
//
// Each exported function corresponds to a single SQL UDF call per row.
// Numeric arguments and return values are passed directly. ClickHouse `String`
// values map to AssemblyScript `string` (UTF-16 in memory, rtId = 2).

// SQL: add(a UInt32, b UInt32) RETURNS UInt32
export function add(a: u32, b: u32): u32 {
  return a + b;
}

// SQL: as_double(x Float64) RETURNS Float64
export function as_double(x: f64): f64 {
  return x * 2.0;
}

// SQL: greet(name String) RETURNS String — echoes a "Hello, <name>!" string back.
export function greet(name: string): string {
  return "Hello, " + name + "!";
}

// SQL: str_repeat(s String, n UInt32) RETURNS String
//      Concatenates `s` with itself `n` times.
export function str_repeat(s: string, n: u32): string {
  let out = "";
  for (let i: u32 = 0; i < n; i++) {
    out += s;
  }
  return out;
}

// SQL: str_length(s String) RETURNS UInt32
//      Returns the number of UTF-16 code units (NOT bytes) in the string.
export function str_length(s: string): u32 {
  return s.length;
}
