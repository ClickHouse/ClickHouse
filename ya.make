# What are "ya.make" files?
# "ya.make" file is a configuration file for "ya" - an internal Yandex build system that is used to build internal Yandex monorepository.
# We don't use nor "ya" build system neither the Yandex monorepository for development of ClickHouse and you should not pay attention to these files.
# But ClickHouse source code is synchronized with internal Yandex monorepository, that's why "ya.make" files exist.

OWNER(g:clickhouse)

RECURSE(
    base
    programs
    src
)
