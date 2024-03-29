//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool flag = false;  // 标志是否找到相应value值
  uint32_t array_size = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < array_size; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      result->emplace_back(array_[i].second);
      flag = true;
    } else if (!IsOccupied(i)) {  // 提前结束寻找
      break;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  uint32_t array_size = BUCKET_ARRAY_SIZE;
  uint32_t pos = array_size;  // 可以插入的位置
  for (uint32_t i = 0; i < array_size; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {  // 是否存在相同的元素
      return false;
    }
    if (!IsReadable(i)) {
      if (pos == array_size) {  // 插入位置尚未设置
        pos = i;
      }
      if (!IsOccupied(i)) {  // 提前结束寻找
        break;
      }
    }
  }
  if (pos == array_size) {  // bucket已满
    return false;
  }
  // 设置kv值，同时设置标志位
  array_[pos].first = key;
  array_[pos].second = value;
  SetOccupied(pos);
  SetReadable(pos);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  uint32_t array_size = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < array_size; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      SetUnreadable(i);  // 将可读位设置为无效
      return true;
    }
    if (!IsOccupied(i)) {  // 提前结束寻找
      break;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return static_cast<bool>(occupied_[index] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[index] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return static_cast<bool>(readable_[index] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] &= ~(1 << offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  uint32_t exact_div_size = BUCKET_ARRAY_SIZE / 8;  // 整除的部分应该全部为ff
  for (uint32_t i = 0; i < exact_div_size; i++) {
    // readable_[i]类型为unsigned char，故可以直接与0xff比较
    if (readable_[i] != 0xff) {
      return false;
    }
  }
  uint32_t rest = BUCKET_ARRAY_SIZE - BUCKET_ARRAY_SIZE / 8 * 8;  // 只有rest个位为1
  unsigned char expect_value = (1 << rest) - 1;
  return !(rest != 0 && readable_[(BUCKET_ARRAY_SIZE - 1) / 8] != expect_value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t cnt = 0;
  uint32_t array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  unsigned char n;
  for (uint32_t i = 0; i < array_size; i++) {
    n = readable_[i];
    while (n != 0) {
      n &= n - 1;  // 将最低位1清0, 计算1的个数
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Size() -> uint32_t {  // 返回桶的大小
  return BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  uint32_t array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (uint32_t i = 0; i < array_size; i++) {
    if (readable_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
