#pragma once

#include <fmt/core.h>

#include <bitset>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <list>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "LSMConfig.hpp"
#include "MurmurHash3.h"
#include "SkipList.hpp"

template <typename K, typename V> struct SSTable {
  K minKey = std::numeric_limits<K>::max(); // 当前segment的最小key
  K maxKey = std::numeric_limits<K>::min(); // 当前segment的最大key
  uint64_t kvPairNum = 0;                   // kv对的数量
  uint64_t lenOfAllValues = 0;              // 所有value长度之和
  std::list<std::pair<K, V>> kvdata;        // 在内存中的所有kv
  std::list<uint64_t> valueOffset;          // value在文件的offset
  std::bitset<BLOOM_SIZE> bloom;            // 布隆过滤器

  SSTable(SkipList<K, V> &li);
  SSTable();
  ~SSTable();

  void clear();
  void writeToFile(std::string filename, uint64_t timeStamp);
};

template <typename K, typename V> SSTable<K, V>::SSTable() {}

template <typename K, typename V> SSTable<K, V>::~SSTable() {}

template <typename K, typename V> SSTable<K, V>::SSTable(SkipList<K, V> &li) {
  assert(bloom.none() == true);

  auto [resMin, minK] = li.getMinKey();
  auto [resMax, maxK] = li.getMaxKey();
  assert(resMin == true);
  assert(resMax == true);
  minKey = minK;
  maxKey = maxK;
  kvPairNum = li.nodeNum();
  // todo: use scanALL() replace minKey&maxKey
  li.scan(minKey, maxKey, kvdata);

  uint64_t offset = 0;
  uint32_t hash[4] = {0};
  for (auto &[k, v] : kvdata) {
    valueOffset.push_back(offset);
    offset += v.size();
    lenOfAllValues += v.size();
    MurmurHash3_x64_128(&k, sizeof(k), 1, hash);
    for (int i = 0; i < 4; ++i) {
      bloom[hash[i] % BLOOM_SIZE] = 1;
    }
  }
}

template <typename K, typename V>
void SSTable<K, V>::writeToFile(std::string filename, uint64_t timeStamp) {
  std::fstream out(filename, std::ios::out | std::ios::binary);
  assert(out.is_open() == true);
  out.write(reinterpret_cast<const char *>(&timeStamp), sizeof(timeStamp));
  out.write(reinterpret_cast<const char *>(&lenOfAllValues),
            sizeof(lenOfAllValues));
  out.write(reinterpret_cast<const char *>(&minKey), sizeof(minKey));
  out.write(reinterpret_cast<const char *>(&maxKey), sizeof(maxKey));
  out.write(reinterpret_cast<const char *>(&kvPairNum), sizeof(kvPairNum));
  out.write(reinterpret_cast<const char *>(&bloom), sizeof(bloom));

  auto it = kvdata.begin();
  auto it2 = valueOffset.begin();
  for (; it != kvdata.end(); ++it, ++it2) {
    out.write(reinterpret_cast<const char *>(&(it->first)), sizeof(it->first));
    out.write(reinterpret_cast<const char *>(&(*it2)), sizeof(*it2));
  }
  for (auto it = kvdata.begin(); it != kvdata.end(); ++it) {
    if constexpr (std::is_same_v<std::string, V>) {
      out.write(it->second.c_str(), it->second.size());
    } else {
      static_assert(1 == 1, "目前只支持V == std::String");
    }
  }

  out.close();
}

template <typename K>
std::string readSSTableFromFile(std::string fileName, uint64_t offset) {
  fmt::print("{}: open {}\n", __FUNCTION__, fileName);
  std::ifstream in(fileName, std::ios::in | std::ios::binary);
  assert(in.is_open() == true);
  uint64_t timeStamp_, lenOfAllValues_, minKey_, maxKey_, kvPairNum_;
  std::bitset<BLOOM_SIZE> bloom_;
  in.read(reinterpret_cast<char *>(&timeStamp_), sizeof(timeStamp_));
  in.read(reinterpret_cast<char *>(&lenOfAllValues_), sizeof(lenOfAllValues_));
  in.read(reinterpret_cast<char *>(&minKey_), sizeof(minKey_));
  in.read(reinterpret_cast<char *>(&maxKey_), sizeof(maxKey_));
  in.read(reinterpret_cast<char *>(&kvPairNum_), sizeof(kvPairNum_));
  in.read(reinterpret_cast<char *>(&bloom_), sizeof(bloom_));

  K key;
  uint64_t off;
  uint64_t targetLen;

  uint64_t i = 0;
  for (; i < kvPairNum_; ++i) {
    in.read(reinterpret_cast<char *>(&key), sizeof(key));
    in.read(reinterpret_cast<char *>(&off), sizeof(off));
    if (offset == off) {
      break;
    }
  }

  if (i < kvPairNum_ - 1) {
    in.read(reinterpret_cast<char *>(&key), sizeof(key));
    in.read(reinterpret_cast<char *>(&off), sizeof(off));
    assert(off > offset);
    targetLen = off - offset;
  } else if (i == kvPairNum_ - 1) {
    std::cout << lenOfAllValues_ << '\n';
    std::cout << offset << '\n';
    assert(lenOfAllValues_ > offset);
    targetLen = lenOfAllValues_ - offset;
  } else {
    return {};
  }

  size_t absOffset = 5 * sizeof(uint64_t) + sizeof(bloom_) +
                     kvPairNum_ * (sizeof(K) + sizeof(uint64_t)) + offset;
  in.seekg(absOffset);
  std::string targetStr;
  targetStr.resize(targetLen);
  in.read(targetStr.data(), targetLen);
  in.close();
  return targetStr;
}

template <typename K, typename V>
std::list<std::tuple<uint32_t, uint64_t, K, V>>
filterSSTableFromFile(uint32_t layer, uint64_t serialNum,
                      std::string fileName) {
  std::ifstream in(fileName, std::ios::in | std::ios::binary);
  fmt::print("filterSSTableFromFile: open file {}\n", fileName);
  assert(in.is_open() == true);
  uint64_t timeStamp_, lenOfAllValues_, minKey_, maxKey_, kvPairNum_;
  std::bitset<BLOOM_SIZE> bloom_;
  in.read(reinterpret_cast<char *>(&timeStamp_), sizeof(timeStamp_));
  in.read(reinterpret_cast<char *>(&lenOfAllValues_), sizeof(lenOfAllValues_));
  in.read(reinterpret_cast<char *>(&minKey_), sizeof(minKey_));
  in.read(reinterpret_cast<char *>(&maxKey_), sizeof(maxKey_));
  in.read(reinterpret_cast<char *>(&kvPairNum_), sizeof(kvPairNum_));
  in.read(reinterpret_cast<char *>(&bloom_), sizeof(bloom_));

  if (kvPairNum_ == 0) {
    return {};
  }

  std::vector<std::pair<K, uint64_t>> keyOffset;
  K key;
  uint64_t off;
  uint64_t targetLen;
  for (size_t i = 0; i < kvPairNum_; ++i) {
    in.read(reinterpret_cast<char *>(&key), sizeof(key));
    in.read(reinterpret_cast<char *>(&off), sizeof(off));
    keyOffset.emplace_back(key, off);
  }
  std::list<std::tuple<uint32_t, uint64_t, K, V>> result;

  for (size_t i = 0; i < keyOffset.size() - 1; ++i) {
    uint64_t valLen = keyOffset[i + 1].second - keyOffset[i].second;
    std::string buf;
    buf.resize(valLen);
    in.read(buf.data(), valLen);
    if constexpr (std::is_same_v<V, std::string>) {
      result.emplace_back(layer, serialNum, keyOffset[i].first, std::move(buf));
    } else {
      V value;
      ::memcpy(&value, buf.c_str(), buf.size());
      static_assert(true, "ensure value's space enough");
      static_assert(true, "todo: support V != std::string");
      /* result.emplace_back(timeStamp_, keyOffset[i].first,
       * std::move(value));*/
    }
  }
  // handle last value
  uint32_t valLen = lenOfAllValues_ - keyOffset.back().second;
  std::string buf;
  buf.resize(valLen);
  in.read(buf.data(), valLen);
  if constexpr (std::is_same_v<V, std::string>) {
    result.emplace_back(layer, serialNum, keyOffset.back().first,
                        std::move(buf));
  } else {
    V value;
    ::memcpy(&value, buf.c_str(), buf.size());
    static_assert(true, "ensure value's space enough");
    static_assert(true, "todo: support V != std::string");
    /* result.emplace_back(timeStamp_, keyOffset.back().first,
     * std::move(value)); */
  }
  in.close();

  return result;
}

template <typename K> struct SummaryOfSSTable {
  uint32_t layer;     // 第几层, 从0开始
  uint64_t serialNum; // sstable序列号
  uint64_t timeStamp;
  K minKey = std::numeric_limits<K>::max();
  K maxKey = std::numeric_limits<K>::min();
  uint64_t kvPairNum = 0;        // kv(offset)的数量
  std::bitset<BLOOM_SIZE> bloom; // 布隆过滤器
  std::vector<std::pair<K, uint64_t>> keyOffset;
  // std::list<std::pair<K, uint64_t>> keyOffset;

  SummaryOfSSTable() {}

  // how to constrict U == K
  template <typename U, typename V>
  requires(std::is_same_v<K, U>)
  SummaryOfSSTable(SSTable<U, V> &st, uint32_t layer_, uint64_t serialNum_,
                   uint64_t timeStamp_)
      : layer(layer_), serialNum(serialNum_), timeStamp(timeStamp_),
        minKey(st.minKey), maxKey(st.maxKey), kvPairNum(st.kvPairNum),
        bloom(st.bloom) {
    // 构建keyOffset
    auto it = st.kvdata.begin();
    auto it2 = st.valueOffset.begin();
    assert(st.kvdata.size() == st.valueOffset.size());
    for (; it2 != st.valueOffset.end(); ++it, ++it2) {
      keyOffset.emplace_back(it->first, *it2);
    }
    assert(keyOffset.size() == st.kvdata.size());
  }

  // todo
  SummaryOfSSTable(const SummaryOfSSTable<K> &rhs)
      : layer(rhs.layer), serialNum(rhs.serialNum), timeStamp(rhs.timeStamp),
        minKey(rhs.minKey), maxKey(rhs.maxKey), kvPairNum(rhs.kvPairNum),
        bloom(rhs.bloom), keyOffset(rhs.keyOffset) {}

  SummaryOfSSTable(SummaryOfSSTable<K> &&rhs)
      : layer(rhs.layer), serialNum(rhs.serialNum), timeStamp(rhs.timeStamp),
        minKey(rhs.minKey), maxKey(rhs.maxKey), kvPairNum(rhs.kvPairNum),
        bloom(rhs.bloom), keyOffset(std::move(rhs.keyOffset)) {}

  SummaryOfSSTable &operator=(const SummaryOfSSTable<K> &rhs) {
    layer = rhs.layer;
    serialNum = rhs.serialNum;
    timeStamp = rhs.timeStamp;
    minKey = rhs.minKey;
    maxKey = rhs.maxKey;
    kvPairNum = rhs.kvPairNum;
    bloom = rhs.bloom;
    keyOffset = rhs.keyOffset;
    return *this;
  }

  SummaryOfSSTable &operator=(SummaryOfSSTable<K> &&rhs) {
    layer = rhs.layer;
    serialNum = rhs.serialNum;
    timeStamp = rhs.timeStamp;
    minKey = rhs.minKey;
    maxKey = rhs.maxKey;
    kvPairNum = rhs.kvPairNum;
    bloom = rhs.bloom;
    keyOffset = std::move(rhs.keyOffset);
    return *this;
  }

  ~SummaryOfSSTable() {}
};

template <typename K>
void readSummaryOfSSTableFromFile(std::string fileName,
                                  SummaryOfSSTable<K> &summary) {
  std::ifstream in(fileName, std::ios::in | std::ios::binary);
  assert(in.is_open() == true);
  uint64_t timeStamp_, lenOfAllValues_, minKey_, maxKey_, kvPairNum_;
  std::bitset<BLOOM_SIZE> bloom_;
  in.read(reinterpret_cast<char *>(&timeStamp_), sizeof(timeStamp_));
  in.read(reinterpret_cast<char *>(&lenOfAllValues_), sizeof(lenOfAllValues_));
  in.read(reinterpret_cast<char *>(&minKey_), sizeof(minKey_));
  in.read(reinterpret_cast<char *>(&maxKey_), sizeof(maxKey_));
  in.read(reinterpret_cast<char *>(&kvPairNum_), sizeof(kvPairNum_));
  in.read(reinterpret_cast<char *>(&bloom_), sizeof(bloom_));

  // todo: like &summary.minKey have align problem? if not, use it
  summary.timeStamp = timeStamp_;
  summary.minKey = minKey_;
  summary.maxKey = maxKey_;
  summary.kvPairNum = kvPairNum_;
  summary.bloom = bloom_;

  K key;
  uint64_t off;

  for (uint64_t i = 0; i < kvPairNum_; ++i) {
    in.read(reinterpret_cast<char *>(&key), sizeof(key));
    in.read(reinterpret_cast<char *>(&off), sizeof(off));
    summary.keyOffset.emplace_back(key, off);
  }
  in.close();
}