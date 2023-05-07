#pragma once

#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <list>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include <bit>
#include <span>

/**
 * + k_len + k + v_len + v + offsets(n对kv则n个offset) + number of kv +
 * + k_len && v_len use uint16_t(2B)+
 */
// 要求该block的offset不能超过2^16次幂

void put_u16(std::vector<uint8_t> &buf, uint16_t value) {
  if constexpr (std::endian::native == std::endian::big) {
    buf.push_back(value >> 8);
    buf.push_back(value & 0xFF);
  } else {
    buf.push_back(value & 0xFF);
    buf.push_back(value >> 8);
  }
}

uint16_t get_u16(uint8_t first, uint8_t second) {
  if constexpr (std::endian::native == std::endian::big) {
    return (first << 8) + second;
  } else {
    return (second << 8) + first;
  }
}

// https://stackoverflow.com/questions/70317885/creating-a-concept-for-a-non-template-class-with-template-methods
struct Block {
  // using kv = std::pair<std::span<uint8_t>, std::span<uint8_t>>;
  using iterator_category = std::random_access_iterator_tag;

  struct kv {
    std::span<uint8_t> key;
    std::span<uint8_t> value;
    friend bool operator==(const kv &x, const kv &y) noexcept {
      if (x.key.size() != y.key.size()) {
        return false;
      }
      for (size_t i = 0; i < x.key.size(); ++i) {
        if (x.key[i] != y.key[i]) {
          return false;
        }
      }
      return true;
    }
  };

  std::vector<uint8_t> data_; // k_len k v_len v
  std::vector<uint16_t> offsets_;

  std::vector<uint8_t> encode() {
    auto buf = data_;
    uint16_t offsets_len = static_cast<uint16_t>(offsets_.size());
    for (auto &offset : offsets_) {
      put_u16(buf, offset);
    }

    put_u16(buf, offsets_len);
    return buf;
  }

  Block &decode(std::vector<uint8_t> data) {
    uint16_t offsets_len =
        *reinterpret_cast<uint16_t *>(&data[data.size() - sizeof(uint16_t)]);
    uint16_t data_end =
        data.size() - sizeof(uint16_t) - offsets_len * sizeof(uint16_t);
    auto offsetSpan = std::span<uint8_t>(data.begin() + data_end,
                                         data.end() - sizeof(uint16_t));
    for (size_t i = 0; i < offsetSpan.size(); i += 2) {
      uint16_t offset = get_u16(offsetSpan[i], offsetSpan[i + 1]);
      this->offsets_.push_back(offset);
    }
    std::copy(data.begin(), data.begin() + data_end, this->data_.begin());
    return *this;
  }

  struct iterator {
    using difference_type = int64_t;

    Block *ptr;
    difference_type index;

    iterator() noexcept : ptr(nullptr), index(0) {}
    iterator(const iterator &i) noexcept : ptr(i.ptr), index(i.index) {}

    iterator(Block *ptr, difference_type index) noexcept
        : ptr(ptr), index(index) {}

    kv operator*() const noexcept {
      assert(index >= 0);
      uint16_t off = ptr->offsets_[index];
      uint16_t k_len = get_u16(ptr->data_[off], ptr->data_[off + 1]);
      off += sizeof(uint16_t);
      std::span<uint8_t> key(ptr->data_.begin() + off, k_len);
      off += k_len;
      uint16_t v_len = get_u16(ptr->data_[off], ptr->data_[off + 1]);
      off += sizeof(uint16_t);
      std::span<uint8_t> value(ptr->data_.begin() + off, v_len);
      return {key, value};
    }

    kv operator[](difference_type n) const noexcept {
      assert(n >= 0);
      uint16_t off = ptr->offsets_[n];
      uint16_t k_len = get_u16(ptr->data_[off], ptr->data_[off + 1]);
      off += sizeof(uint16_t);
      std::span<uint8_t> key(ptr->data_.begin() + off, k_len);
      off += k_len;
      uint16_t v_len = get_u16(ptr->data_[off], ptr->data_[off + 1]);
      off += sizeof(uint16_t);
      std::span<uint8_t> value(ptr->data_.begin() + off, v_len);
      return {key, value};
    }

    iterator &operator++() noexcept {
      ++index;
      return *this;
    }

    iterator operator++(int) noexcept {
      iterator tmp = *this;
      ++index;
      return tmp;
    }

    iterator &operator--() noexcept {
      --index;
      return *this;
    }

    iterator operator--(int) noexcept {
      iterator tmp = *this;
      --index;
      return tmp;
    }

    iterator &operator+=(difference_type n) noexcept {
      index += n;
      return *this;
    }

    iterator &operator-=(difference_type n) noexcept {
      index -= n;
      return *this;
    }

    iterator operator+(difference_type n) const noexcept {
      return iterator{this->ptr, this->index + n};
    }

    iterator operator-(difference_type n) const noexcept {
      return iterator{this->ptr, this->index - n};
    }

    friend difference_type operator-(const iterator &x,
                                     const iterator &y) noexcept {
      assert(x.ptr == y.ptr);
      assert(x.ptr != nullptr && y.ptr != nullptr);
      return x.index - y.index;
    }

    friend bool operator==(const iterator &x, const iterator &y) noexcept {
      return x.ptr == y.ptr && x.index == y.index;
    }

    friend bool operator!=(const iterator &x, const iterator &y) noexcept {
      return x.ptr == y.ptr && x.index != y.index;
    }
  };

  auto begin() { return iterator{this, 0}; }

  auto end() {
    return iterator{
        this, static_cast<iterator::difference_type>(this->offsets_.size())};
  }
};

struct BlockBuilder {
  std::vector<uint8_t> data_;
  std::vector<uint16_t> offsets_;
  uint16_t block_size_;

  BlockBuilder(uint16_t block_size) : block_size_(block_size) {}

  uint16_t estimated_size() {
    return offsets_.size() * sizeof(uint16_t) + data_.size() + sizeof(uint16_t);
  }

  // 退化到uint8_t?
  bool add(std::span<uint8_t> key, std::span<uint8_t> value) {
    assert(key.empty() == false);
    if (estimated_size() + key.size() + value.size() + sizeof(uint16_t) * 3 >
            block_size_ &&
        !empty()) {
      // k_len/v_len/add a offset(all uint16_t)
      return false;
    }

    offsets_.emplace_back(static_cast<uint16_t>(data_.size()));
    put_u16(data_, static_cast<uint16_t>(key.size()));
    for (auto &u8 : key) {
      data_.emplace_back(u8);
    }
    put_u16(data_, static_cast<uint16_t>(value.size()));
    for (auto &u8 : value) {
      data_.emplace_back(u8);
    }
    return true;
  }

  bool empty() { return offsets_.empty(); }

  Block build() {
    if (empty()) {
      std::abort();
    }

    return Block{.data_ = this->data_, .offsets_ = this->offsets_};
  }
};

/**
 * 迭代器:参考std::list和std::vector的++/+=之类的操作
 */