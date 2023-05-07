#include <catch2/catch_test_macros.hpp>
#include <fmt/format.h>

#include <bitset>
#include <concepts>
#include <iostream>
#include <type_traits>
#include <vector>

#include <sys/time.h>

#include "Cache.hpp"
#include "SSTable.hpp"
#include "SkipList.hpp"

TEST_CASE("test_SummaryOfSSTable", "test_SummaryOfSSTable") {

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

  SSTable<size_t, std::string> st(list);

  struct timeval now;
  ::gettimeofday(&now, nullptr);
  uint64_t timeStamp = 1000000 * now.tv_sec + now.tv_usec;

  uint32_t layer = 1;
  uint32_t serialNum = 2;
  SummaryOfSSTable<size_t> summary(st, layer, serialNum, timeStamp);
  REQUIRE(summary.layer == layer);
  REQUIRE(summary.serialNum == serialNum);
  REQUIRE(summary.timeStamp == timeStamp);
  REQUIRE(summary.minKey == st.minKey);
  REQUIRE(summary.maxKey == st.maxKey);
  REQUIRE(summary.kvPairNum == st.kvPairNum);
  REQUIRE(st.bloom == summary.bloom);
  REQUIRE(&(st.bloom) != &(summary.bloom));

  auto it = st.kvdata.begin();
  auto it2 = summary.keyOffset.begin();
  uint64_t offset = 0;
  for (; it2 != summary.keyOffset.end(); ++it2, ++it) {
    REQUIRE(it2->first == it->first);
    REQUIRE(it2->second == offset);
    offset += it->second.size();
  }

  std::vector<SummaryOfSSTable<size_t>> vec;
  vec.push_back(summary);

  auto &ref = vec.front();
  REQUIRE(ref.layer == layer);
  REQUIRE(ref.serialNum == serialNum);
  REQUIRE(ref.timeStamp == timeStamp);
  REQUIRE(ref.minKey == st.minKey);
  REQUIRE(ref.maxKey == st.maxKey);
  REQUIRE(ref.kvPairNum == st.kvPairNum);
  REQUIRE(st.bloom == ref.bloom);
  REQUIRE(&(st.bloom) != &(ref.bloom));

  auto new_it = st.kvdata.begin();
  auto new_it2 = ref.keyOffset.begin();
  uint64_t new_offset = 0;
  for (; new_it2 != ref.keyOffset.end(); ++new_it2, ++new_it) {
    REQUIRE(new_it2->first == new_it->first);
    REQUIRE(new_it2->second == new_offset);
    new_offset += new_it->second.size();
  }
}

TEST_CASE("test_Cache", "test_Cache") {
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

  SSTable<size_t, std::string> st(list);

  struct timeval now;
  ::gettimeofday(&now, nullptr);
  uint64_t timeStamp = 1000000 * now.tv_sec + now.tv_usec;

  uint32_t layer = 1;
  uint32_t serialNum = 2;
  SummaryOfSSTable<size_t> summary(st, layer, serialNum, timeStamp);

  Cache<size_t> cache;
  cache.insert(summary);
  cache.insert(st, layer, serialNum, timeStamp);
  REQUIRE(cache.cacheOfLayer.size() == 2);
  for (size_t i = start; i < range; ++i) {
    // 因为从start开始, 所以需要减去start
    auto tmpOffset = summary.keyOffset[i - start].second;
    auto [layer_, serialNum_, offset_] = cache.search(i);
    REQUIRE(layer_ == layer);
    REQUIRE(serialNum_ == serialNum);
    REQUIRE(offset_ == tmpOffset);
  }
  // 搜索不到
  auto [layer_, serialNum_, offset_] = cache.search(range);
  REQUIRE(layer_ == LSM_MAX_LAYER + 1);
  REQUIRE(serialNum_ == 0);
  REQUIRE(offset_ == 0);

  cache.delByTimestamp(timeStamp);
  REQUIRE(cache.cacheOfLayer.size() == 1);
  cache.delByTimestamp(timeStamp);
  REQUIRE(cache.cacheOfLayer.size() == 0);
}

TEST_CASE("filterSSTableFromFile", "filterSSTableFromFile") {
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
  uint64_t timeStamp = 1000000 * now.tv_sec + now.tv_usec;
  table.writeToFile("sstable_test.txt", timeStamp);
  auto result =
      filterSSTableFromFile<uint64_t, std::string>(0, 0, "sstable_test.txt");

  size_t cnt = start;
  for (auto &[layer, serialNum, key, val] : result) {
    REQUIRE(key == cnt);
    REQUIRE(val == fmt::format("key = {}, value = {}", cnt, cnt));
    ++cnt;
  }
}