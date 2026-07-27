#ifndef __wasilibc___struct_in6_addr_h
#define __wasilibc___struct_in6_addr_h
/// wasi-libc's `in6_addr` exposes only `s6_addr`. Poco::Net also reads the BSD-style
/// `__u6_addr` union members, so the WASM experiment supplies the conventional layout.
#include <stdint.h>

struct in6_addr {
  union {
    unsigned char __s6_addr[16];
    uint16_t __s6_addr16[8];
    uint32_t __s6_addr32[4];
  } __in6_union;
};
#define s6_addr __in6_union.__s6_addr
#define __u6_addr __in6_union
#define __u6_addr8 __s6_addr
#define __u6_addr16 __s6_addr16
#define __u6_addr32 __s6_addr32

#endif
