set(BASE_HEADERS
    "SkipList.hpp"
    "Random.hpp"
    "SSTable.hpp"
    "LSMConfig.hpp"
    "MurmurHash3.h"
    "Cache.hpp"
    "KVStore.hpp"
    "Block.hpp"
    )
set(BASE_SRCS
    "SkipList.cpp"
    "SSTable.cpp"
    "MurmurHash3.cpp"
    "Cache.cpp"
    "KVStore.cpp"
    )

add_library(kvbase STATIC ${BASE_HEADERS} ${BASE_SRCS})

target_link_libraries(kvbase PRIVATE fmt::fmt)

target_include_directories(kvbase
    PUBLIC
        ./
)