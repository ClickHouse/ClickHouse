UNITTEST_FOR(metrika/core/libs/uatraits-fast)

OWNER(g:metrika-core)

PEERDIR(
    library/unittest
    metrika/core/libs/statdaemons
    metrika/core/libs/uatraits-fast
)

INCLUDE(${ARCADIA_ROOT}/metrika/core/include_dirs.inc)

ADDINCL(${ARCADIA_BUILD_ROOT}/metrika/core/libs/uatraits-fast/tests)

SRCDIR(metrika/core/libs/uatraits-fast/tests)

SRCS(
    uatraits-user-agent.cpp
)

DATA(
    sbr://1363472933 # data/browser.xml data/profiles.xml data/extra.xml
)

END()
