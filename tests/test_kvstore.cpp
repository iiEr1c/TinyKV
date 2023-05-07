#include <catch2/catch_test_macros.hpp>

#include <fmt/core.h>
#include <fmt/format.h>

#include "KVStore.hpp"

struct A {
  std::array<int, 16> arr{};
};

TEST_CASE("test_kvstore", "test_kvstore") {

  auto baseDir = std::string("./kv_directory/");
  KVStore<uint64_t, std::string> kv(baseDir);

  uint16_t start = 1;
  // 1024 * 16, key = 1丢失
  uint32_t end = 1024 * 16;

  // REQUIRE(true == kv.put(1, fmt::format("key = {}, value = {}", 1, 1)));
  // auto [ret, value] = kv.get(1);
  // REQUIRE(ret == true);
  // REQUIRE(value == fmt::format("key = {}, value = {}", 1, 1));

  /*
  for (auto i = start; i < end; ++i) {
    REQUIRE(true == kv.put(i, fmt::format("key = {}, value = {}", i, i)));
  }

  for (auto i = start; i < end; ++i) {
    auto [ret, value] = kv.get(i);
    REQUIRE(ret == true);
    REQUIRE(value == fmt::format("key = {}, value = {}", i, i));
  }

  for (auto i = start; i < end / 2; ++i) {
    REQUIRE(true == kv.del(i));
  }

  auto [ret, value] = kv.get(end / 2 - 1);
  REQUIRE(ret == false);

  for (auto i = start; i < end / 2; ++i) {
    fmt::print("i= {}\n", i);
    auto [ret, value] = kv.get(i);
    if (value.size() > 0) {
      fmt::print("value = {}\n", value);
    }
    REQUIRE(ret == false);
  }
  */

  // for (auto i = end; i < end * 2; ++i) {
  //   auto [ret, value] = kv.get(i);
  //   REQUIRE(ret == false);
  // }

  // test skiplist

  /*
  SkipList<uint64_t, std::string> skipList;
  for (auto i = start; i < end; ++i) {
    REQUIRE(true == skipList.insert(i, fmt::format("key = {}, value = {}", i,
  i)));
  }

  for (auto i = start; i < end; ++i) {
    auto [ret, value] = skipList.search(i);
    REQUIRE(ret == true);
    REQUIRE(value == fmt::format("key = {}, value = {}", i, i));
  }
  */
}

TEST_CASE("test_endian", "test_endian") {
  std::vector<uint8_t> vec;
  uint16_t num = 0xAA55;
  REQUIRE(0xAA == (num >> 8));
  REQUIRE(0x55 == (num & 0xFF));
  
  vec.push_back(static_cast<uint8_t>(num & 0xFF));
  vec.push_back(static_cast<uint8_t>(num >> 8));

  REQUIRE(0xAA == vec[1]);
  REQUIRE(0x55 == vec[0]);

  uint16_t read_num;
  read_num = *reinterpret_cast<uint16_t *>(vec.data());
  REQUIRE(read_num == num);
}