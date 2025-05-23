cmake_minimum_required(VERSION 3.10)
project(crossdb)
add_compile_options(-O2)

option(BUILD_SHARED_LIBS "Build using shared libraries" ON)
option(ENABLE_CLI "Build CLI executable" ON)
option(ENABLE_PACKAGE "Build package" ON)

add_library(crossdb src/crossdb.c)
IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
    target_link_libraries(crossdb pthread ws2_32 regex)
ENDIF ()
set_target_properties(crossdb PROPERTIES PUBLIC_HEADER "include/crossdb.h")

#add_subdirectory(jni)
include(GNUInstallDirs)
INSTALL(
	TARGETS crossdb
    EXPORT crossdb-export
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

INSTALL(
    EXPORT crossdb-export
    FILE crossdb-config.cmake
    NAMESPACE crossdb::
    DESTINATION lib/cmake/crossdb
)

if(ENABLE_CLI)
    add_executable(xdb-cli src/xdb-cli.c)
    IF (CMAKE_SYSTEM_NAME MATCHES "Windows")
        target_link_libraries(xdb-cli pthread ws2_32 regex)
    ELSE ()
        target_link_libraries(xdb-cli pthread dl)
    ENDIF ()
    INSTALL(
	    TARGETS xdb-cli
    	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()

add_custom_target("uninstall" COMMENT "Uninstall installed files")
add_custom_command(
    TARGET "uninstall"
    POST_BUILD
    COMMENT "Uninstall files with install_manifest.txt"
    COMMAND xargs rm -vf < install_manifest.txt || echo Nothing in
            install_manifest.txt to be uninstalled!
)

if(ENABLE_PACKAGE)
    set(CPACK_PACKAGE_NAME crossdb)
    set(CPACK_PACKAGE_VERSION_MAJOR 0)
    set(CPACK_PACKAGE_VERSION_MINOR 14)
    set(CPACK_PACKAGE_VERSION_PATCH 0)
    set(CPACK_PACKAGE_CONTACT "crossdb <support@crossdb.org>")
    #set(CPACK_GENERATOR DEB NSIS RPM)
    IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(CPACK_GENERATOR DEB ZIP)
    ELSE ()
        set(CPACK_GENERATOR ZIP)
    ENDIF ()
    include(CPack)
endif()
