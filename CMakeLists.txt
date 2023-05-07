cmake_minimum_required(VERSION 3.15)

project(KV VERSION 0.1
    DESCRIPTION "KV"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

include(FetchContent)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

set(CMAKE_VERBOSE_MAKEFILE off)

# release需要手动关闭
set(IS_ENABLE_SANITIZE on CACHE BOOL "" FORCE)

# 暂时关闭
#list(APPEND CXX_FLAGS
#    "-fno-rtti")

list(APPEND SANITIZE_FLAG "")

if(IS_ENABLE_SANITIZE)
    list(APPEND SANITIZE_FLAG
        "-fno-omit-frame-pointer"
        "-fsanitize=address"
        "-fsanitize=leak"
        "-fsanitize=undefined")
endif()

list(APPEND CXX_FLAGS_DEBUG
    "-Og"
    "-DDBUG")

# "-Wsuggest-final-types"
# "-Wsuggest-final-methods"
# "-Wsuggest-override")
if(IS_ENABLE_SANITIZE)
    list(APPEND CXX_FLAGS_DEBUG ${SANITIZE_FLAG})
endif()

list(APPEND CXX_FLAGS_RELEASE
    "-O3"
    "-Wall"
    "-Wextra"
    "-Werror"
    "-Wconversion"
    "-Wshadow"
    "-Wno-unused-parameter"
    "-Wno-unused")

include(3rd/fmt.cmake)

# todo: 是否需要手动实现静态编译
SET(CMAKE_FIND_LIBRARY_PREFIXES "lib")
SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".so")


# include(3rd/fmt.cmake)
# The compiled library code is here
add_subdirectory(src)

# The executable code is here
add_subdirectory(apps)

enable_testing()

# if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    include(3rd/catch2.cmake)
    add_subdirectory(tests)
endif()
