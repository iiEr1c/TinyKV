#pragma once

#include <cstddef>
#include <cstdint>

constexpr bool isPowerOf2(size_t N) {
  size_t n = 1;
  while (n < N) {
    n *= 2;
  }
  return n == N;
}

constexpr uint32_t LSM_MAX_LAYER = 16; // LSM最大深度
constexpr uint32_t KB = 1024;
constexpr uint32_t MB = 1024 * 1024;
inline constexpr size_t BLOOM_SIZE = 8 * KB; // 布隆过滤器大小
//inline constexpr size_t MEM_LIMIT = MB;      // 内存限制

inline constexpr size_t MEM_LIMIT = 16 * KB;      // 内存限制

static_assert(isPowerOf2(BLOOM_SIZE), "BLOOM_SIZE must be power of 2");