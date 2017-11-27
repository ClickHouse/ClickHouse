
add_executable(corrector_utf8 corrector_utf8.cpp)

# Link the executable to the library.
target_link_libraries (corrector_utf8 clickhouse_common_io)

install( TARGETS corrector_utf8 RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT corrector_utf8)
