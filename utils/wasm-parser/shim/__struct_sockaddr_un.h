#ifndef __wasilibc___struct_sockaddr_un_h
#define __wasilibc___struct_sockaddr_un_h
/// wasi-libc declares `sockaddr_un` without `sun_path`, because it has no Unix sockets.
/// Poco::Net names the field in an inline accessor, so the WASM experiment supplies the
/// conventional layout. No Unix socket is ever created here.
#include <__typedef_sa_family_t.h>

struct sockaddr_un {
  __attribute__((aligned(__BIGGEST_ALIGNMENT__))) sa_family_t sun_family;
  char sun_path[108];
};

#endif
