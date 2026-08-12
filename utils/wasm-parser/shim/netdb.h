#pragma once
/// Minimal <netdb.h> shim for the WASM experiment: wasi-libc has no name resolution.
/// Only declarations are provided - Poco::Net headers must compile, but nothing calls these.
#include <stdint.h>
#include <sys/socket.h>

#ifdef __cplusplus
extern "C" {
#endif

struct hostent { char * h_name; char ** h_aliases; int h_addrtype; int h_length; char ** h_addr_list; };
struct addrinfo
{
    int ai_flags, ai_family, ai_socktype, ai_protocol;
    socklen_t ai_addrlen;
    struct sockaddr * ai_addr;
    char * ai_canonname;
    struct addrinfo * ai_next;
};

#define AI_PASSIVE 0x01
#define AI_CANONNAME 0x02
#define AI_NUMERICHOST 0x04
#define AI_NUMERICSERV 0x400
#define AI_ADDRCONFIG 0x20
#define AI_ALL 0x10
#define AI_V4MAPPED 0x08
#define NI_MAXHOST 255
#define NI_MAXSERV 32
#define NI_NUMERICHOST 0x01
#define NI_NUMERICSERV 0x02
#define NI_NAMEREQD 0x08
#define NI_DGRAM 0x10
#define EAI_AGAIN (-3)
#define EAI_BADFLAGS (-1)
#define EAI_FAIL (-4)
#define EAI_FAMILY (-6)
#define EAI_MEMORY (-10)
#define EAI_NONAME (-2)
#define EAI_SERVICE (-8)
#define EAI_SOCKTYPE (-7)
#define EAI_SYSTEM (-11)
#define EAI_OVERFLOW (-12)
#define HOST_NOT_FOUND 1
#define TRY_AGAIN 2
#define NO_RECOVERY 3
#define NO_DATA 4

int getaddrinfo(const char *, const char *, const struct addrinfo *, struct addrinfo **);
void freeaddrinfo(struct addrinfo *);
int getnameinfo(const struct sockaddr *, socklen_t, char *, socklen_t, char *, socklen_t, int);
const char * gai_strerror(int);
struct hostent * gethostbyname(const char *);
struct hostent * gethostbyaddr(const void *, socklen_t, int);
extern int h_errno;

#ifdef __cplusplus
}
#endif
