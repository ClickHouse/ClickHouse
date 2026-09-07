#pragma once
/// Wraps wasi-libc's <fenv.h>. WebAssembly has no configurable rounding mode, so wasi-libc omits
/// the FE_* rounding constants. Poco's FPEnvironment_C99 names them; the WASM build never
/// changes the rounding mode, so nominal values suffice.
#include_next <fenv.h>

#ifndef FE_TONEAREST
#define FE_TONEAREST 0
#endif
#ifndef FE_DOWNWARD
#define FE_DOWNWARD 0x400
#endif
#ifndef FE_UPWARD
#define FE_UPWARD 0x800
#endif
#ifndef FE_TOWARDZERO
#define FE_TOWARDZERO 0xc00
#endif

#ifndef FE_DIVBYZERO
#define FE_DIVBYZERO 0x02
#endif
#ifndef FE_INEXACT
#define FE_INEXACT 0x20
#endif
#ifndef FE_INVALID
#define FE_INVALID 0x01
#endif
#ifndef FE_OVERFLOW
#define FE_OVERFLOW 0x04
#endif
#ifndef FE_UNDERFLOW
#define FE_UNDERFLOW 0x10
#endif
#ifndef FE_ALL_EXCEPT
#define FE_ALL_EXCEPT (FE_DIVBYZERO | FE_INEXACT | FE_INVALID | FE_OVERFLOW | FE_UNDERFLOW)
#endif
