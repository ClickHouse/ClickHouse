#pragma once
/// Minimal <net/if.h> shim for the WASM experiment: wasi-libc has no network interface API.
#define IF_NAMESIZE 16
#define IFNAMSIZ IF_NAMESIZE
#ifdef __cplusplus
extern "C" {
#endif
struct if_nameindex { unsigned int if_index; char * if_name; };
unsigned int if_nametoindex(const char *);
char * if_indextoname(unsigned int, char *);
struct if_nameindex * if_nameindex(void);
void if_freenameindex(struct if_nameindex *);
#ifdef __cplusplus
}
#endif
