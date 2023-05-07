#pragma once

#include "LSMConfig.hpp"
#include "MurmurHash3.h"
#include "SSTable.hpp"

#include <algorithm>
#include <bitset>
#include <concepts>
#include <list>
#include <tuple>
#include <type_traits>

template <typename K> struct Cache {
  std::list<SummaryOfSSTable<K>> cacheOfLayer;

  // <layer, serialNum, offset In Values>
  using SearchResultType = std::tuple<uint32_t, uint64_t, uint64_t>;

  Cache() {}
  ~Cache() {}

  template <typename U, typename V>
  requires(std::is_same_v<K, U>)
  void insert(SSTable<U, V> &table, uint32_t layer_, uint64_t serialNum_,
              uint64_t timeStamp_) {
    SummaryOfSSTable<U> summary(table, layer_, serialNum_, timeStamp_);
    cacheOfLayer.push_front(summary);
  }

  void insert(const SummaryOfSSTable<K> &summary) {
    cacheOfLayer.push_front(summary);
  }

  // todo:能否优化?
  bool delByTimestamp(uint64_t timeStamp) {
    for (auto it = cacheOfLayer.begin(); it != cacheOfLayer.end(); ++it) {
      if (it->timeStamp == timeStamp) {
        cacheOfLayer.erase(it);
        return true;
      }
    }
    return false;
  }

  template <typename U>
  requires(std::is_same_v<K, U>)
  SearchResultType search(U key) {
    uint32_t hash[4] = {0};
    for (auto it = cacheOfLayer.begin(); it != cacheOfLayer.end(); ++it) {
      // 判断区间是否符合
      if (key > it->maxKey || key < it->minKey) {
        continue;
      }

      MurmurHash3_x86_128(&key, sizeof(key), 1, hash);
      // todo: use simd
      if (it->bloom[hash[0] % BLOOM_SIZE] == '0')
        continue;
      if (it->bloom[hash[1] % BLOOM_SIZE] == '0')
        continue;
      if (it->bloom[hash[2] % BLOOM_SIZE] == '0')
        continue;
      if (it->bloom[hash[3] % BLOOM_SIZE] == '0')
        continue;
      // std::pair<K, uint64_t>
      auto result = std::lower_bound(
          it->keyOffset.begin(), it->keyOffset.end(), key,
          [](auto &&left, U value) { return left.first < value; });
      if (result != it->keyOffset.end()) {
        fmt::print("result->first = {}, result->second = {}\n", result->first,
                   result->second);
        if (key == result->first) {
          fmt::print("low_bound found: key = {}\n", key);
          return {it->layer, it->serialNum, result->second};
        } else {
          fmt::print("low_bound not found: key = {}\n", key);
          return {LSM_MAX_LAYER + 1, 0, 0};
        }
      } else {
        fmt::print("low_bound not found: key = {}\n", key);
        return {LSM_MAX_LAYER + 1, 0, 0};
      }
    }

    return {LSM_MAX_LAYER + 1, 0, 0};
  }

  void clear() { cacheOfLayer.clear(); }

  size_t size() { return cacheOfLayer.size(); }

  auto begin() { return cacheOfLayer.begin(); }
  auto end() { return cacheOfLayer.end(); }

  const auto cbegin() const { return cacheOfLayer.cbegin(); }
  const auto cend() const { return cacheOfLayer.cend(); }
};