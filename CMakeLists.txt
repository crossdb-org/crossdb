cmake_minimum_required(VERSION 3.10)
project(crossdb)
add_compile_options(-O2)
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)
add_library(crossdb src/crossdb.c)
add_executable(xdb-cli src/xdb-cli.c)
IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
target_link_libraries(xdb-cli pthread ws2_32)
target_link_libraries(crossdb pthread ws2_32)
ELSE ()
target_link_libraries(xdb-cli pthread dl)
ENDIF ()
set_target_properties(crossdb PROPERTIES PUBLIC_HEADER "include/crossdb.h")

#add_subdirectory(jni)

INSTALL(
	TARGETS xdb-cli crossdb
	RUNTIME DESTINATION bin
	LIBRARY DESTINATION lib
	PUBLIC_HEADER DESTINATION include
)

add_custom_target("uninstall" COMMENT "Uninstall installed files")
add_custom_command(
    TARGET "uninstall"
    POST_BUILD
    COMMENT "Uninstall files with install_manifest.txt"
    COMMAND xargs rm -vf < install_manifest.txt || echo Nothing in
            install_manifest.txt to be uninstalled!
)

set(CPACK_PACKAGE_NAME crossdb)
set(CPACK_PACKAGE_VERSION_MAJOR 0)
set(CPACK_PACKAGE_VERSION_MINOR 12)
set(CPACK_PACKAGE_VERSION_PATCH 0)
set(CPACK_PACKAGE_CONTACT "crossdb <support@crossdb.org>")
#set(CPACK_GENERATOR DEB NSIS RPM)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
set(CPACK_GENERATOR DEB)
ENDIF ()
set(CPACK_GENERATOR ZIP)
include(CPack)