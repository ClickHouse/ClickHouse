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

// SQL: concat3(a String, b String, c String) RETURNS String
//      Returns `a + sep + b + sep + c`. Used to exercise the multi-String argument
//      path of the AS ABI: each argument is allocated on the WASM heap by `__new`,
//      and the host must pin earlier arguments while later ones are allocated so the
//      AS GC does not collect them under memory pressure.
export function concat3(a: string, b: string, c: string): string {
  return a + "|" + b + "|" + c;
}
