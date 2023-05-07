#include <catch2/catch_test_macros.hpp>
#include <fmt/format.h>

#include <algorithm>

#include "Block.hpp"

TEST_CASE("test_Block", "test_Block") {
  BlockBuilder builder(4096);
  size_t begin = 0, end = 16;
  for (size_t i = begin; i < end; ++i) {
    std::string key = fmt::format("key_{}", i);
    std::string value = fmt::format("value_{}", i);
    // fmt::print("{}: {}\n", key, value);
    std::span<uint8_t> keySpan(reinterpret_cast<uint8_t *>(key.data()),
                               key.size());

    std::span<uint8_t> valueSpan(reinterpret_cast<uint8_t *>(value.data()),
                                 value.size());
    REQUIRE(true == builder.add(keySpan, valueSpan));
  }
  auto block = builder.build();

  auto it = block.begin();

  for (auto it = block.begin(); it != block.end(); ++it) {
    auto [k, v] = *it;
    std::string key;
    key.resize(k.size());
    std::string value;
    value.resize(v.size());
    ::memcpy(key.data(), k.data(), k.size());
    ::memcpy(value.data(), v.data(), v.size());
    fmt::print("{}: {}\n", key, value);
  }
}