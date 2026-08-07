#pragma once
/// Wraps wasi-libc's <signal.h>, adding the POSIX bits it omits that ClickHouse headers mention
/// in signatures (Common/StackTrace.h). Nothing in the WebAssembly build handles signals.
#include_next <signal.h>

#ifndef __WASM_SHIM_SIGINFO
#define __WASM_SHIM_SIGINFO
#ifdef __cplusplus
extern "C" {
#endif
typedef struct siginfo_t
{
    int si_signo, si_errno, si_code;
    int si_pid, si_uid, si_status;
    void * si_addr;
} siginfo_t;
#ifdef __cplusplus
}
#endif
#endif
