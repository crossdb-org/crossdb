cmake_minimum_required(VERSION 2.8.3)
project(crossdbjni)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
include_directories(include)
include_directories(../include)

link_directories(./lib)


add_library(crossdbjni-win64 SHARED db_jni.cpp)
target_link_libraries(crossdbjni-win64 PRIVATE crossdb)