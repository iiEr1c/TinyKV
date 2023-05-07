#include <catch2/catch_test_macros.hpp>

#include <fmt/format.h>

#include <bitset>
#include <fstream>
#include <iostream>
#include <list>
#include <string>

#include <sys/time.h>

#include "SSTable.hpp"
#include "SkipList.hpp"

TEST_CASE("skiplist", "skiplist") {
  SkipList<uint64_t, std::string> list;
  constexpr size_t range = 1024 * 16;
  constexpr size_t start = 1; // 总是从1开始, 0作为头节点
  // 0 和 0xffffffffffffffff作为哨兵使用
  for (size_t i = start; i < range; ++i) {
    REQUIRE(true == list.insert(i, fmt::format("key = {}, value = {}", i, i)));
  }
  REQUIRE(list.nodeNum() == range - start);
  REQUIRE(list.getMinKey().second == start);
  REQUIRE(list.getMaxKey().second == range - start);

  for (size_t i = start; i < range; ++i) {
    REQUIRE(true == list.search(i).first);
  }
  for (size_t i = range; i < range * 2; ++i) {
    REQUIRE(false == list.search(i).first);
  }

  for (size_t i = start; i < range / 2; ++i) {
    REQUIRE(true == list.remove(i));
  }
  for (size_t i = start; i < range / 2; ++i) {
    REQUIRE(false == list.search(i).first);
  }

  std::list<std::pair<uint64_t, std::string>> result;
  size_t startKey = 1, endKey = 64;
  list.scan(startKey, endKey, result);
  REQUIRE(0 == result.size());

  result.clear();
  startKey = range / 2;
  endKey = startKey + 1024;
  list.scan(startKey, endKey, result);
  REQUIRE(endKey - startKey + 1 == result.size());

  list.clear();
  REQUIRE(list.search(std::numeric_limits<size_t>::max()).first == true);
}

TEST_CASE("test_SkipList_same_key", "test_SkipList_same_key") {
  SkipList<uint64_t, std::string> list;
  constexpr size_t range = 1024 * 16;
  constexpr size_t key = 1;
  // 0 和 0xffffffffffffffff作为哨兵使用
  for (size_t i = 0; i < range; ++i) {
    list.insert(key, fmt::format("key = {}, value = {}", key, i));
  }
  REQUIRE(list.nodeNum() == 1);
}

TEST_CASE("bitset2", "bitset2") {
  // 说明不是简单的除8, 为了简单直接强制要求必须是2的n次幂即可.
  std::bitset<16 * 1024> bs;
  std::cout << "sizeof(bs) = " << sizeof(bs) << '\n';
  REQUIRE(sizeof(bs) == 2048);
}

TEST_CASE("bitset", "bitset") {
  constexpr size_t SIZE = 1024 * 1024 * 8;
  std::bitset<SIZE> bloom;
  Random rand(0x12345678);
  for (size_t i = 0; i < SIZE; ++i) {
    bloom[i] = rand.Uniform(2);
  }
  std::ofstream out("./test.txt", std::ios::out | std::ios::binary);
  REQUIRE(out.is_open() == true);
  REQUIRE(sizeof(bloom) == SIZE / 8);
  out.write(reinterpret_cast<const char *>(&bloom), sizeof(bloom));
  out.close();

  std::ifstream in("./test.txt", std::ios::in | std::ios::binary);
  std::bitset<SIZE> bloom2;
  REQUIRE(sizeof(bloom2) == SIZE / 8);
  in.read(reinterpret_cast<char *>(&bloom2), sizeof(bloom2));
  for (size_t i = 0; i < SIZE; ++i) {
    REQUIRE(bloom[i] == bloom2[i]);
  }
  in.close();
}

TEST_CASE("test_SSTable", "test_SSTable") {
  SkipList<uint64_t, std::string> list;
  constexpr size_t range = 128;
  constexpr size_t start = 1; // 总是从1开始, 0作为头节点
  size_t valueLen = 0;
  for (auto i = start; i < range; ++i) {
    auto str = fmt::format("key = {}, value = {}", i, i);
    valueLen += str.size();
    REQUIRE(true == list.insert(i, str));
  }

  REQUIRE(list.nodeNum() == range - start);
  REQUIRE(list.getMinKey().second == start);
  REQUIRE(list.getMaxKey().second == range - start);

  SSTable table(list);
  REQUIRE(table.minKey == start);
  REQUIRE(table.maxKey == range - start);
  REQUIRE(table.kvPairNum == list.nodeNum());
  REQUIRE(table.lenOfAllValues == valueLen);

  struct timeval now;
  ::gettimeofday(&now, nullptr);
  table.writeToFile("sstable_test.txt",
                    uint64_t(1000000 * now.tv_sec + now.tv_usec));

  auto str = readSSTableFromFile<uint64_t>("sstable_test.txt", 0);
  REQUIRE(str.size() == 18);
  REQUIRE(str == std::string("key = 1, value = 1"));
  str = readSSTableFromFile<uint64_t>("sstable_test.txt", 18);
  REQUIRE(str == std::string("key = 2, value = 2"));

  // 尝试读取最后一个value
  auto lastStr = fmt::format("key = {}, value = {}", range - 1, range - 1);
  std::cout << "valueLen = " << valueLen << '\n';
  std::cout << "lastStr's len = " << lastStr.size() << '\n';
  str = readSSTableFromFile<uint64_t>("sstable_test.txt",
                                      valueLen - lastStr.size());
  REQUIRE(str == lastStr);
}