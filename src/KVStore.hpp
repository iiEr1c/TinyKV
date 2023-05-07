#pragma once

#include <fmt/core.h>

#include "Cache.hpp"
#include "LSMConfig.hpp"
#include "SSTable.hpp"
#include "SkipList.hpp"

#include <array>
#include <cassert>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;

template <typename K, typename V> struct KVStore {
  using LayerSerial = std::pair<uint32_t, uint64_t>;

  KVStore(std::string dataDirectory = "./");

  ~KVStore();

  bool put(K key, V value);

  std::pair<bool, V> get(K key);

  bool del(K key);

private:
  void compaction();

  void mergeLayer(uint32_t curLayer);

  void writeSSTToNextLayer(uint32_t curLayer, uint64_t timeStamp);

  void mergeAllFiles(const std::vector<LayerSerial> &inputFiles,
                     std::list<std::tuple<uint32_t, uint64_t, K, V>> &out);

  std::tuple<K, K, uint64_t>
  scanLayerSSTByOffset(uint32_t curLayer, uint64_t offset,
                       std::vector<LayerSerial> &out);

  uint64_t SSTNeedMergedNextLayer(uint32_t curLayer, K curLayerMinKey,
                                  K curLayerMaxKey,
                                  std::vector<LayerSerial> &out);

  void init();

  void readWAL();

  void writeWAL(K key, const V &value);

  void clearWAL();

  uint64_t loadSSTToCache(uint32_t layerTh, const std::string &sstName);

  void readSSTDataToCache();

  std::string genSSTNameByLayer(uint32_t layer);

  std::string genSSTNameBySerialNum(uint64_t serialNum);

  std::string genLayerDir(uint32_t layer);

  static uint64_t getNumBySSTFilename(const std::string &sstFileName);

  uint64_t MaxSSTFileNumberInLayer(uint32_t layer);

  bool layerSSTExceedLimit(uint32_t layer);

private:
  SkipList<K, V> memTable;   // LSM的内存层
  SkipList<K, V> mergeTable; // for merge
  std::array<Cache<K>, LSM_MAX_LAYER> diskTableCache =
      {};                    // 磁盘文件的k-v's offset
  std::string diskDir;       // 磁盘文件根目录
  std::array<uint64_t, LSM_MAX_LAYER> availableNum =
      {};                    // 每一层下一个可用编号, init=0
  uint32_t depthOfLayer = 0; // LSM层数, 以0开始计算
  uint64_t curTimeStamp = 0; // 每生成一个sst都增加curTimeStamp
};

template <typename K, typename V>
KVStore<K, V>::KVStore(std::string dataDirectory) : diskDir(dataDirectory) {
  if (!fs::exists(diskDir)) {
    fs::create_directory(diskDir);
  }

  std::string dataPath = diskDir + std::string("data/");
  if (!fs::exists(dataPath)) {
    fs::create_directory(dataPath);
  }

  std::string walPath = diskDir + std::string("log/");
  if (!fs::exists(walPath)) {
    fs::create_directory(walPath);
  }

  // 从磁盘读数据和日志(如果有)
  init();
}

template <typename K, typename V> KVStore<K, V>::~KVStore() {
  if (memTable.nodeNum() > 0) {
    SSTable<K, V> ss(memTable);
    uint32_t curLayer = 0;
    auto dirPath = genLayerDir(curLayer);
    auto sstName = genSSTNameByLayer(curLayer);
    if (!fs::exists(dirPath)) {
      fs::create_directory(dirPath);
    }
    fmt::print(
        "minKey = {}, maxKey = {}, kvPairNum = {}, lenOfAllValues = {}\n",
        ss.minKey, ss.maxKey, ss.kvPairNum, ss.lenOfAllValues);
    fmt::print("============>ss.writeToFile({}, {})\n", dirPath + sstName,
               curTimeStamp);
    // 写入cache
    diskTableCache[curLayer].insert(ss, curLayer, availableNum[curLayer],
                                    curTimeStamp);
    ss.writeToFile(dirPath + sstName, curTimeStamp);
    ++availableNum[curLayer];
    ++curTimeStamp;
    compaction();

    memTable.clear();
    clearWAL();
  }
}

template <typename K, typename V> void KVStore<K, V>::init() {
  readSSTDataToCache();
  readWAL();
}

template <typename K, typename V> bool KVStore<K, V>::put(K key, V value) {
  if (memTable.getMemSize() + sizeof(K) + value.size() < MEM_LIMIT) {
    writeWAL(key, value);
    return memTable.insert(std::move(key), std::move(value));
  } else {
    const uint32_t layer = 0; // 表示正在操作第layer层的sst
    // todo: sue SummaryOfSSTable接口?, 然后后续允许修改SSTable
    SSTable<K, V> sst(memTable);
    diskTableCache[layer].insert(sst, layer, availableNum[layer], curTimeStamp);
    std::string layerPath = genLayerDir(layer);
    std::string sstName = genSSTNameByLayer(layer);
    if (!fs::exists(layerPath)) {
      fs::create_directory(layerPath);
    }
    sst.writeToFile(layerPath + sstName, curTimeStamp);
    ++availableNum[layer];
    ++curTimeStamp; // 用于表示sst的顺序

    compaction();

    memTable.clear();

    clearWAL();
    writeWAL(key, value);
    return memTable.insert(std::move(key), std::move(value));
  }
}

template <typename K, typename V> std::pair<bool, V> KVStore<K, V>::get(K key) {
  auto [hasKey, value] = memTable.search(key);
  if constexpr (std::is_same_v<V, std::string>) {
    if (hasKey) {
      if (value == std::string("~DELETED~")) {
        fmt::print("{}, {}, get({}) == false\n", __FUNCTION__, __LINE__, key);
        return {false, V{}};
      } else {
        return {true, value};
      }
    }

    // 内存表中不存在,需要从sst中搜索
    uint32_t layer;
    uint64_t serialNum;
    uint64_t offset;
    for (uint32_t i = 0; i <= depthOfLayer; ++i) {
      std::tie(layer, serialNum, offset) = diskTableCache[i].search(key);
      fmt::print("line: {}, layer = {}, serialNum = {}, offset = {}\n",
                 __LINE__, layer, serialNum, offset);
      if (layer != LSM_MAX_LAYER + 1) {
        break;
      }
    }

    // 不存在该key
    if (layer == LSM_MAX_LAYER + 1) {
      fmt::print("line: {}, {}, {}, get({}) == false\n", __LINE__, __FUNCTION__,
                 __LINE__, key);
      return {false, V{}};
    }

    fmt::print("line: {}, layer = {}, serialNum = {}, offset = {}\n", __LINE__,
               layer, serialNum, offset);
    auto sstFilename = genLayerDir(layer) + genSSTNameBySerialNum(serialNum);
    std::string value = readSSTableFromFile<K>(sstFilename, offset);

    if (value != std::string("~DELETED~")) {
      return {true, value};
    } else {
      fmt::print("{}, {}, get({}) == false\n", __FUNCTION__, __LINE__, key);
      return {false, V{}};
    }
  } else {
    fmt::print("todo: support V != std::string\n");
    fmt::print("{}, {}, get({}) == false\n", __FUNCTION__, __LINE__, key);
    return {false, V{}};
  }
}

template <typename K, typename V> bool KVStore<K, V>::del(K key) {
  auto [ret, value] = get(key);
  if (ret == false) {
    return false; // key不存在
  }
  if constexpr (std::is_same_v<V, std::string>) {
    if (value == std::string("~DELETED~")) {
      return true;
    }
    memTable.remove(key);
    put(key, std::string("~DELETED~"));
    return true;
  } else {
    fmt::print("todo: support V != std::string\n");
    return false;
  }
}

template <typename K, typename V> void KVStore<K, V>::compaction() {
  uint32_t curLayer = 0;
  while (layerSSTExceedLimit(curLayer)) {
    mergeLayer(curLayer);
    ++curLayer;
  }
}

template <typename K, typename V>
void KVStore<K, V>::mergeLayer(uint32_t curLayer) {
  std::vector<LayerSerial> mergeFiles; // 使用层数+顺序号表示sst文件
  uint64_t curLayerNeedScanStart = 0;
  if (curLayer > 0) {
    curLayerNeedScanStart = MaxSSTFileNumberInLayer(curLayer);
  }

  auto [minKey, maxKey, curMaxTimestamp] =
      scanLayerSSTByOffset(curLayer, curLayerNeedScanStart, mergeFiles);
  auto eraseIt = diskTableCache[curLayer].begin();
  std::advance(eraseIt, curLayerNeedScanStart);
  diskTableCache[curLayer].cacheOfLayer.erase(eraseIt,
                                              diskTableCache[curLayer].end());

  auto tmpMaxTimestamp =
      SSTNeedMergedNextLayer(curLayer, minKey, maxKey, mergeFiles);

  curMaxTimestamp =
      curMaxTimestamp > tmpMaxTimestamp ? curMaxTimestamp : tmpMaxTimestamp;

  std::list<std::tuple<uint32_t, uint64_t, K, V>> resultOfMerge;
  mergeAllFiles(mergeFiles, resultOfMerge);

  mergeTable.clear();
  auto prevKey = std::numeric_limits<K>::max();

  bool curLayerIsBottom = curLayer == depthOfLayer;
  for (auto &[layer_, serialNum_, key, val] : resultOfMerge) {
    // 只保留最新的key
    if (key != prevKey) {
      if constexpr (std::is_same_v<V, std::string>) {
        if (curLayerIsBottom && val == std::string("~DELETED~")) {
          prevKey = key;
          continue;
        }
      } else {
        static_assert(true, "todo: support V != std::string\n");
        std::cout << "typeId(V) = " << typeid(V).name() << '\n';
      }

      if (mergeTable.getMemSize() + sizeof(key) + val.size() < MEM_LIMIT) {
        mergeTable.insert(key, val);
        prevKey = key;
      } else {
        writeSSTToNextLayer(curLayer, curMaxTimestamp);
        mergeTable.insert(key, val); // 别忘了插入数据
        prevKey = key;
      }
    }
  }

  if (mergeTable.nodeNum() > 0) {
    writeSSTToNextLayer(curLayer, curMaxTimestamp);
  }

  for (auto &file : mergeFiles) {
    fs::remove(genLayerDir(file.first) + genSSTNameBySerialNum(file.second));
  }
}

template <typename K, typename V>
void KVStore<K, V>::writeSSTToNextLayer(uint32_t curLayer, uint64_t timeStamp) {
  uint32_t nextLayer = curLayer == LSM_MAX_LAYER ? LSM_MAX_LAYER : curLayer + 1;
  SSTable<K, V> sst(mergeTable);
  diskTableCache[nextLayer].insert(sst, nextLayer, availableNum[nextLayer],
                                   timeStamp);
  auto levelDir = genLayerDir(nextLayer);
  auto sstName = genSSTNameByLayer(nextLayer);
  if (!fs::exists(levelDir)) {
    fs::create_directory(levelDir);
    depthOfLayer = nextLayer; // 更新当前LSM的最大深度.
  }
  sst.writeToFile(levelDir + sstName, curTimeStamp);
  ++availableNum[nextLayer];
  mergeTable.clear();
}

template <typename K, typename V>
void KVStore<K, V>::mergeAllFiles(
    const std::vector<LayerSerial> &inputFiles,
    std::list<std::tuple<uint32_t, uint64_t, K, V>> &out) {
  for (auto &file : inputFiles) {
    auto tmp = filterSSTableFromFile<K, V>(
        file.first, file.second,
        genLayerDir(file.first) + genSSTNameBySerialNum(file.second));
    out.merge(tmp, [](auto &&left, auto &&right) {
      auto LLayer = std::get<0>(left);
      auto LSerialNum = std::get<1>(left);
      auto LKey = std::get<2>(left);
      auto LValue = std::get<3>(left);

      auto RLayer = std::get<0>(right);
      auto RSerialNum = std::get<1>(right);
      auto RKey = std::get<2>(right);
      auto RValue = std::get<3>(right);

      if (LKey == RKey) {
        if (LLayer < RLayer) {
          return true;
        } else if (LLayer > RLayer) {
          return false;
        } else {
          return LSerialNum > RSerialNum;
        }
      } else {
        return LKey < RKey;
      }
    });
  }
}

template <typename K, typename V>
std::tuple<K, K, uint64_t>
KVStore<K, V>::scanLayerSSTByOffset(uint32_t curLayer, uint64_t offset,
                                    std::vector<LayerSerial> &out) {
  K minKey = std::numeric_limits<K>::max();
  K maxKey = std::numeric_limits<K>::min();
  uint64_t maxTimestamp = 0;

  auto it = diskTableCache[curLayer].begin();
  std::advance(it, offset);

  for (; it != diskTableCache[curLayer].end(); ++it) {
    assert(it->layer == curLayer);
    out.emplace_back(it->layer, it->serialNum);
    maxTimestamp = maxTimestamp > it->timeStamp ? maxTimestamp : it->timeStamp;
    minKey = minKey < it->minKey ? minKey : it->minKey;
    maxKey = maxKey > it->maxKey ? maxKey : it->maxKey;
  }
  return {minKey, maxKey, maxTimestamp};
}

template <typename K, typename V>
uint64_t KVStore<K, V>::SSTNeedMergedNextLayer(uint32_t curLayer,
                                               K curLayerMinKey,
                                               K curLayerMaxKey,
                                               std::vector<LayerSerial> &out) {
  uint64_t maxTimestamp = 0;
  if (curLayer == LSM_MAX_LAYER) {
    return maxTimestamp;
  }
  uint32_t nextLayer = curLayer + 1;

  auto it = diskTableCache[nextLayer].begin();
  for (; it != diskTableCache[nextLayer].end();) {
    K minK = it->minKey;
    K maxK = it->maxKey;
    if (curLayerMaxKey < minK || maxK < curLayerMinKey) {
      ++it;
      continue; // 无交集
    }
    assert(it->layer == nextLayer);
    out.emplace_back(it->layer, it->serialNum);
    maxTimestamp = maxTimestamp > it->timeStamp ? maxTimestamp : it->timeStamp;
    it = diskTableCache[nextLayer].cacheOfLayer.erase(it);
  }

  return maxTimestamp;
}

template <typename K, typename V> void KVStore<K, V>::readWAL() {
  assert(diskDir.size() > 0);
  auto walLogPath = diskDir + std::string("log/wal.log");
  if (fs::exists(walLogPath)) {
    std::ifstream in(walLogPath, std::ios::in);
    assert(in.is_open());
    K key;
    uint64_t valueLen;
    while (!in.eof()) {
      in.read(reinterpret_cast<char *>(&key), sizeof(key));
      in.read(reinterpret_cast<char *>(&valueLen), sizeof(valueLen));
      std::string buf;
      buf.resize(valueLen);
      in.read(buf.data(), valueLen);
      if constexpr (std::is_same_v<std::string, V>) {
        put(key, std::move(buf));
      } else {
        // todo: std::string convert to V
        V *ptr = reinterpret_cast<V *>(buf.data);
        V value = *ptr; // 多了一次拷贝
        put(key, std::move(value));
      }
    }

    in.close();
    clearWAL();
  }
}

template <typename K, typename V>
void KVStore<K, V>::writeWAL(K key, const V &value) {
  assert(diskDir.size() > 0);
  auto logDir = diskDir + std::string("log/");
  if (!fs::exists(logDir)) {
    fs::create_directory(logDir);
  }
  auto logFileName = std::string("wal.log");
  std::ofstream out(logDir + logFileName,
                    std::ios::out | std::ios::app | std::ios::binary);
  uint64_t valueLen = value.size();
  out.write(reinterpret_cast<char *>(&key), sizeof(key));
  out.write(reinterpret_cast<char *>(&valueLen), sizeof(valueLen));
  out.write(reinterpret_cast<const char *>(&value), sizeof(V));
  out.close();
}

template <typename K, typename V> void KVStore<K, V>::clearWAL() {
  auto walLogPath = diskDir + std::string("log/wal.log");
  fs::remove(walLogPath);
}

template <typename K, typename V>
uint64_t KVStore<K, V>::loadSSTToCache(uint32_t layerTh,
                                       const std::string &sstName) {
  auto dataDir = genLayerDir(layerTh);
  auto sstPath = dataDir + sstName;
  std::fstream in(sstPath, std::ios::in);
  if (!in.is_open()) [[unlikely]] {
    std::abort();
  }
  SummaryOfSSTable<K> summary;
  readSummaryOfSSTableFromFile<K>(sstPath, summary);
  fmt::print("{}: readSummaryOfSSTableFromFile {}\n", __FUNCTION__, sstPath);
  summary.layer = layerTh;
  summary.serialNum = getNumBySSTFilename(sstName);
  uint64_t timeStamp = summary.timeStamp;
  fmt::print("summary: layer = {}, serialNum = {},timeStamp = {},minKey = "
             "{},maxKey = {}, kvPairNum = {}\n",
             summary.layer, summary.serialNum, summary.timeStamp,
             summary.minKey, summary.maxKey, summary.kvPairNum);
  diskTableCache[layerTh].insert(std::move(summary));
  return timeStamp;
}

template <typename K, typename V> void KVStore<K, V>::readSSTDataToCache() {
  std::vector<uint64_t> allSSTSerialNum;
  uint32_t i = 0;
  for (; i < LSM_MAX_LAYER; ++i) {
    std::string layerPath = genLayerDir(i);
    if (!fs::exists(layerPath)) {
      break;
    }
    depthOfLayer = i;

    try {
      allSSTSerialNum.clear();
      for (auto &&iter : fs::directory_iterator(layerPath)) {
        // ignore directory
        if (fs::is_directory(iter.path())) {
          continue;
        }
        allSSTSerialNum.emplace_back(
            getNumBySSTFilename(iter.path().filename().string()));
      }
    } catch (const std::exception &error) {
      // ...
    }

    std::sort(allSSTSerialNum.begin(), allSSTSerialNum.end(),
              std::greater<uint64_t>{});
    // load sst to cache
    for (const auto &sstSerialNum : allSSTSerialNum) {
      auto sstName = genSSTNameBySerialNum(sstSerialNum);
      fmt::print("{}: read {}\n", __FUNCTION__, sstName);
      uint64_t timeStamp = loadSSTToCache(i, sstName);
      curTimeStamp = curTimeStamp > timeStamp ? curTimeStamp : timeStamp;
    }
    if (!allSSTSerialNum.empty()) {
      availableNum[i] = allSSTSerialNum.front() + 1;
    }
    fmt::print("==========>availableNum[{}] = {}\n", i, availableNum[i]);
  }
}

template <typename K, typename V>
std::string KVStore<K, V>::genLayerDir(uint32_t layer) {
  return diskDir + std::string("data/level-") + std::to_string(layer) +
         std::string("/");
}

template <typename K, typename V>
std::string KVStore<K, V>::genSSTNameByLayer(uint32_t layer) {
  return std::string("sst_") + std::to_string(availableNum[layer]) +
         std::string(".sst");
}

template <typename K, typename V>
std::string KVStore<K, V>::genSSTNameBySerialNum(uint64_t serialNum) {
  return std::string("sst_") + std::to_string(serialNum) + std::string(".sst");
}

template <typename K, typename V>
uint64_t KVStore<K, V>::getNumBySSTFilename(const std::string &sstFileName) {
  size_t pos = sstFileName.find(".sst");
  assert(pos != std::string::npos);
  uint64_t serialNum;
  std::from_chars(sstFileName.data() + 4, sstFileName.data() + pos, serialNum);
  fmt::print("sstFileName = {} and serialNum = {}\n", sstFileName, serialNum);
  return serialNum;
}

template <typename K, typename V>
uint64_t KVStore<K, V>::MaxSSTFileNumberInLayer(uint32_t layer) {
  return 2 << layer;
}

template <typename K, typename V>
bool KVStore<K, V>::layerSSTExceedLimit(uint32_t layer) {
  fmt::print("diskTableCache[{}].size() = {}\n", layer,
             diskTableCache[layer].size());
  return diskTableCache[layer].size() > MaxSSTFileNumberInLayer(layer);
}