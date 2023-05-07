add_executable(test_skiplist test_skiplist.cpp)
target_compile_options(test_skiplist PRIVATE
    ${CXX_FLAGS}
    "$<$<CONFIG:Debug>:${CXX_FLAGS_DEBUG}>"
    "$<$<CONFIG:Release>:${CXX_FLAGS_RELEASE}>")

# target_compile_options(app PRIVATE "-fsanitize=address" "-fsanitize=undefined")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_link_options(test_skiplist PRIVATE ${SANITIZE_FLAG})
endif()

# target_link_libraries(app PRIVATE TinyJson)
target_link_libraries(test_skiplist PRIVATE kvbase Catch2::Catch2WithMain fmt::fmt)

add_executable(test_cache test_cache.cpp)
target_compile_options(test_cache PRIVATE
    ${CXX_FLAGS}
    "$<$<CONFIG:Debug>:${CXX_FLAGS_DEBUG}>"
    "$<$<CONFIG:Release>:${CXX_FLAGS_RELEASE}>")

# target_compile_options(app PRIVATE "-fsanitize=address" "-fsanitize=undefined")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_link_options(test_cache PRIVATE ${SANITIZE_FLAG})
endif()

# target_link_libraries(app PRIVATE TinyJson)
target_link_libraries(test_cache PRIVATE kvbase Catch2::Catch2WithMain fmt::fmt)

add_executable(test_kvstore test_kvstore.cpp)
target_compile_options(test_kvstore PRIVATE
    ${CXX_FLAGS}
    "$<$<CONFIG:Debug>:${CXX_FLAGS_DEBUG}>"
    "$<$<CONFIG:Release>:${CXX_FLAGS_RELEASE}>")

# target_compile_options(app PRIVATE "-fsanitize=address" "-fsanitize=undefined")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_link_options(test_kvstore PRIVATE ${SANITIZE_FLAG})
endif()

# target_link_libraries(app PRIVATE TinyJson)
target_link_libraries(test_kvstore PRIVATE kvbase Catch2::Catch2WithMain fmt::fmt)

add_executable(test_Block test_Block.cpp)
target_compile_options(test_Block PRIVATE
    ${CXX_FLAGS}
    "$<$<CONFIG:Debug>:${CXX_FLAGS_DEBUG}>"
    "$<$<CONFIG:Release>:${CXX_FLAGS_RELEASE}>")

# target_compile_options(app PRIVATE "-fsanitize=address" "-fsanitize=undefined")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_link_options(test_Block PRIVATE ${SANITIZE_FLAG})
endif()

# target_link_libraries(app PRIVATE TinyJson)
target_link_libraries(test_Block PRIVATE kvbase Catch2::Catch2WithMain fmt::fmt)