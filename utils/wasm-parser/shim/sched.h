#pragma once
/// Minimal <sched.h> shim for the WASM experiment: WASI has no scheduler API.
/// Poco's Thread_POSIX.h names these constants; no thread is ever created here.
#define SCHED_OTHER 0
#define SCHED_FIFO 1
#define SCHED_RR 2

#ifdef __cplusplus
extern "C" {
#endif
struct sched_param { int sched_priority; };
int sched_get_priority_min(int);
int sched_get_priority_max(int);
int sched_yield(void);
#ifdef __cplusplus
}
#endif
