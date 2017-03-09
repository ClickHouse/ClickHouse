include_directories (${ClickHouse_SOURCE_DIR}/dbms/include)

add_executable(corrector_utf8 corrector_utf8.cpp)

# Link the executable to the library.
target_link_libraries (corrector_utf8 dbms)
add_dependencies(corrector_utf8 dbms)

INSTALL( TARGETS corrector_utf8 RUNTIME DESTINATION bin COMPONENT corrector_utf8)
