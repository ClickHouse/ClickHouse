#pragma once
/// Minimal <pwd.h> shim for the WASM experiment: WASI has no user database.
#include <sys/types.h>
#ifdef __cplusplus
extern "C" {
#endif
struct passwd { char * pw_name; char * pw_passwd; uid_t pw_uid; gid_t pw_gid; char * pw_gecos; char * pw_dir; char * pw_shell; };
/// WASI has no user/permission model either; declared so Poco::Path compiles.
uid_t getuid(void);
uid_t geteuid(void);
struct passwd * getpwuid(uid_t);
struct passwd * getpwnam(const char *);
int getpwuid_r(uid_t, struct passwd *, char *, size_t, struct passwd **);
int getpwnam_r(const char *, struct passwd *, char *, size_t, struct passwd **);
#ifdef __cplusplus
}
#endif
