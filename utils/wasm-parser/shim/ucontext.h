#pragma once
/// Minimal <ucontext.h> shim for the WASM experiment: WASI has no machine contexts.
/// Enough for headers that mention `ucontext_t` in signatures (Common/StackTrace.h);
/// the corresponding stack-unwinding code is not built for WebAssembly.
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct { int __unused_regs[1]; } mcontext_t;

typedef struct ucontext_t
{
    unsigned long uc_flags;
    struct ucontext_t * uc_link;
    struct { void * ss_sp; int ss_flags; size_t ss_size; } uc_stack;
    mcontext_t uc_mcontext;
} ucontext_t;

#ifdef __cplusplus
}
#endif
