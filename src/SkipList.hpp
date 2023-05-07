#pragma once

#include <fmt/core.h>

#include "Random.hpp"

#include <array>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <limits>
#include <list>
#include <optional>
#include <tuple>
#include <utility>

#include <iostream>

// todo: 如何O(1)获取skiplist的最大key和最小key
template <typename Key, typename Value> struct SkipList {
  SkipList() : rand_(0x12345678) {
    // 创建tail_节点, 它节点的level=0
    // tail节点作为哨兵节点, 避免判断!= nullptr
    tail_ = new Node<Key, Value>(std::numeric_limits<Key>::max(), Value{}, 0);
    header_ = new Node<Key, Value>(std::numeric_limits<Key>::min(), Value{},
                                   MAX_LEVEL);
    assert(tail_ != nullptr);
    assert(header_ != nullptr);
    for (size_t i = 0; i <= MAX_LEVEL; ++i) {
      header_->forward_[i] = tail_;
    }
  }
  ~SkipList() {
    Node<Key, Value> *tmp = nullptr;
    while (header_ != nullptr) {
      tmp = header_;
      header_ = header_->forward_[0];
      delete tmp;
    }
  }

  auto getLevel() { return curLevel_; }
  auto getMemSize() { return curMemSize; }

  auto nodeNum() { return nodeCount_; }

  bool insert(Key key, Value value);
  std::pair<bool, Value> search(Key key);
  bool remove(Key key);
  void scan(Key start, Key end, std::list<std::pair<Key, Value>> &li);

  void clear();

  // todo: add scanALL()

  // O(1)
  std::pair<bool, Key> getMinKey() {
    if (nodeCount_ == 0) [[unlikely]] {
      return {false, std::numeric_limits<Key>::max()};
    }
    return {true, header_->forward_[0]->key_};
  }
  // O(n)
  std::pair<bool, Key> getMaxKey() {
    if (nodeCount_ == 0) [[unlikely]] {
      return {false, std::numeric_limits<Key>::max()};
    }
    Node<Key, Value> *current = header_;
    for (int i = 0; i < nodeCount_; ++i) {
      current = current->forward_[0];
    }
    return {true, current->key_};
  }

private:
  template <typename U, typename V> struct Node {
    U key_;
    V value_;
    Node<U, V> **forward_;
    int nodeLevel_; // 用于标识forward的大小

    // 只要 forward_[i] == nullptr说明到达这条链表的结尾
    // 给定key和value和level创建节点.
    // 注意, 这里的level是从0开始计数
    Node(U u, V v, int level)
        : key_(std::move(u)), value_(std::move(v)), forward_(nullptr),
          nodeLevel_(level) {
      forward_ = new Node<U, V> *[level + 1] { nullptr };
      assert(forward_ != nullptr);
      for (int i = 0; i <= level; ++i) {
        assert(forward_[i] == nullptr);
      }
    }

    ~Node() {
      for (int i = 0; i <= nodeLevel_; ++i) {
        forward_[i] = nullptr;
      }
      delete[] forward_;
      forward_ = nullptr;
    }
  };

  using NodeType = Node<Key, Value>;
  using NodeTypePtr = Node<Key, Value> *;

  int randomLevel() {
    int level = static_cast<int>(rand_.Uniform(MAX_LEVEL));
    if (level == 0) {
      level = 1;
    }
    return level;
  }

private:
  uint64_t curMemSize = 0; // 用于记录当前skipList的内存占用大小
  Node<Key, Value> *header_ = nullptr; // key should be Key::min
  Node<Key, Value> *tail_ = nullptr;   // key should be key::max
  size_t nodeCount_ = 0;
  int curLevel_ = 0; // 整个SkipList的最大level, 从0开始计数
  Random rand_;

private:
  static constexpr uint32_t MAX_LEVEL = 16; // 调表最大深度
};

// should not insert Key's min and Key's max
template <typename Key, typename Value>
bool SkipList<Key, Value>::insert(Key key, Value value) {
  std::array<NodeTypePtr, SkipList<Key, Value>::MAX_LEVEL> update{};
  assert(update[1] == nullptr);

  NodeTypePtr current = header_;
  // 找到每一行最后一个小于key的节点&更新到update中
  // assume key ∈(0, 0xffffffffffffffff)
  for (int i = curLevel_; i >= 0; --i) {
    while (current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
    update[i] = current;
  }
  // 此时的current位于第0行最后一个小于key的节点
  current = current->forward_[0];

  // 如果此时current为nullptr呢?
  // 如果key存在, 更新value&内存占用然后返回false
  if (current->key_ == key) {
    // 因为是无符号数, 避免出现负数的情况
    curMemSize -= current->value_.size();
    curMemSize += value.size();
    current->value_ = std::move(value);
    return false;
  }

  int rlevel = SkipList<Key, Value>::randomLevel();
  // 如果随机的层次更深，那么需要将我们的header节点(dummy节点)指向正确的位置(这里记录在update中，后续会更新)，并更新curLevel
  // 因为头节点始终小于新节点.
  if (rlevel > curLevel_) {
    // 从curLevel_ + 1开始, 该节点的前一个节点是header_
    for (int i = curLevel_ + 1; i < rlevel + 1; ++i) {
      update[i] = header_;
    }
    curLevel_ = rlevel;
  }
  NodeType *newNode = new NodeType(key, std::move(value), rlevel);
  assert(newNode != nullptr);
  for (int i = 0; i <= rlevel; ++i) {
    newNode->forward_[i] = update[i]->forward_[i];
    update[i]->forward_[i] = newNode;
  }

  // 更新内存暂用&节点数量
  curMemSize += (sizeof(Key) + value.size());
  ++nodeCount_;

  return true;
}

template <typename Key, typename Value>
bool SkipList<Key, Value>::remove(Key key) {
  std::array<NodeTypePtr, SkipList<Key, Value>::MAX_LEVEL> update{};
  assert(update[1] == nullptr);
  NodeTypePtr current = header_;
  // 找到每一行最后一个小于key的节点&更新到update中
  for (int i = curLevel_; i >= 0; --i) {
    while (current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
    update[i] = current;
  }
  current = current->forward_[0];
  // 节点存在才删除
  if (current->key_ == key) {
    // 需要自底向上删除节点
    for (int i = 0; i <= curLevel_; ++i) {
      // 找到该节点的最顶层即可
      assert(update[i] != nullptr);
      if (update[i]->forward_[i] != current) {
        break;
      }
      update[i]->forward_[i] = current->forward_[i];
    }

    // 删除可能会降低树的高度
    // 更新SkipList整体的高度,
    // 其实高度就等于header_->forward中不为tail_的高度
    while (curLevel_ > 0 && header_->forward_[curLevel_] == tail_) {
      --curLevel_;
    }
    // 更新内存暂用&节点数量
    curMemSize -= (sizeof(Key) + current->value_.size());
    delete current; // 释放内存
    --nodeCount_;
    return true;
  }

  return false;
}


template <typename Key, typename Value>
std::pair<bool, Value> SkipList<Key, Value>::search(Key key) {
  std::array<NodeTypePtr, SkipList<Key, Value>::MAX_LEVEL> update{};
  assert(update[1] == nullptr);
  NodeTypePtr current = header_;
  for (int i = curLevel_; i >= 0; --i) {
    while (current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
    update[i] = current;
  }
  current = current->forward_[0];
  // 如果存在
  if (current->key_ == key) {
    return {true, current->value_};
  }
  return {false, {}};
}

// assume start <= end
template <typename Key, typename Value>
void SkipList<Key, Value>::scan(Key startKey, Key endKey,
                                std::list<std::pair<Key, Value>> &li) {
  assert(startKey <= endKey);
  NodeTypePtr current = header_;
  for (int i = curLevel_; i >= 0; --i) {
    while (current->forward_[i]->key_ < startKey) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  li.clear();
  // if startKey == 0xffffffffffffffff
  if (current == nullptr) [[unlikely]] {
    return;
  }
  while (current->key_ <= endKey && current != tail_) {
    li.emplace_back(current->key_, current->value_);
    current = current->forward_[0];
  }
}

/**
 * 将所有的kv清空
 */
template <typename Key, typename Value> void SkipList<Key, Value>::clear() {
  NodeTypePtr current = header_->forward_[0];
  while (current != tail_) {
    NodeTypePtr tmp = current;
    current = current->forward_[0];
    delete tmp;
  }
  // 修正header的指针
  for (size_t i = 0; i <= MAX_LEVEL; ++i) {
    header_->forward_[i] = tail_;
  }
  curMemSize = 0;
  nodeCount_ = 0;
  curLevel_ = 0;
}