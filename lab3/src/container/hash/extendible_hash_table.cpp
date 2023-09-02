//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  /*
  将目录页永久地驻留在缓存中，但未在析构函数中unpin，实际上是存在问题的
  但我觉得每个操作前fetch目录，操作完成后再unpin太难看了，并且效率不高
  但由于测试方法各对象的析构顺序为:
  delete disk_manager;
  delete bpm;
  delete hash table
  故无法在hash table或BufferPoolManager的析构函数中写入缓存页
  我之前版本的代码是每个操作前fetch目录，操作完成后再unpin
  */
  dir_page_ = CreateDirectoryPage(&directory_page_id_);  // 创建目录页

  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);  // 申请第一个桶的页
  dir_page_->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);  // 放回桶页
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page_) -> uint32_t {
  uint32_t index = Hash(key) & dir_page_->GetGlobalDepthMask();
  return index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page_) -> uint32_t {
  uint32_t index = KeyToDirectoryIndex(key, dir_page_);
  page_id_t page_id = dir_page_->GetBucketPageId(index);
  return page_id;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::CreateDirectoryPage(page_id_t *bucket_page_id) -> HashTableDirectoryPage * {
  auto new_dir_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());  // 创建目录页
  return new_dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::CreateBucketPage(page_id_t *bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto *new_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(bucket_page_id, nullptr)->GetData());
  return new_bucket_page;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page_);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);  // 读取桶页内容前加页的读锁
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page_);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool ret = bucket_page->Insert(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  table_latch_.RUnlock();
  if (!ret && bucket_page->IsFull()) {  // 该桶已满，插入失败
    ret = SplitInsert(transaction, key, value);
  }

  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  bool ret;
  // 待分裂桶的各项信息,称待分离桶为旧桶，申请的桶为新桶
  uint32_t old_bucket_page_index = KeyToDirectoryIndex(key, dir_page_);
  page_id_t old_bucket_page_id = KeyToPageId(key, dir_page_);
  auto *old_bucket_page = FetchBucketPage(old_bucket_page_id);
  uint32_t local_depth = dir_page_->GetLocalDepth(old_bucket_page_index);

  if (!old_bucket_page->IsFull()) {  // 再次检查桶是否满了
    ret = old_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, true, nullptr);
    table_latch_.WUnlock();
    return ret;
  }

  page_id_t new_bucket_page_id;
  auto *new_bucket_page = CreateBucketPage(&new_bucket_page_id);

  uint32_t old_local_mask = dir_page_->GetLocalDepthMask(old_bucket_page_index);  // 之前的掩码，例如111
  uint32_t new_local_mask = old_local_mask + (old_local_mask + 1);  // 计算新的掩码，比之前多一位，例如1111
  uint32_t old_local_hash = old_bucket_page_index & new_local_mask;  // 分裂后旧桶对应的hash值 例1011
  uint32_t new_local_hash = old_local_hash ^ (old_local_mask + 1);  // 分裂后新桶对应的hash值 例0011，对其最高位取反
  uint32_t dir_size = dir_page_->Size();

  // 首先遍历一遍目录，将仍指向旧桶的位置深度加一
  // for (uint32_t i = 0; i < dir_size; i++) {
  //   if ((i & new_local_mask) == old_local_hash) {
  //     dir_page_->IncrLocalDepth(i);
  //   }
  // }
  // 实现与上面代码一样的功能
  for (uint32_t i = old_local_hash; i < dir_size; i += new_local_mask + 1) {
    dir_page_->IncrLocalDepth(i);
  }

  // 而后依据是否影响全局深度，对各位置进行操作
  if (local_depth < dir_page_->GetGlobalDepth()) {  // 不影响目录大小，只是将一半指向旧桶的指针改向新桶
    // for (uint32_t i = 0; i < dir_size; i++) {
    //   page_id = dir_page_->GetBucketPageId(i);
    //   if (page_id == old_bucket_page_id &&
    //       (i & new_local_mask) != old_local_hash) {  // 与旧桶不再一致，将目录指向新桶并将深度加一
    //     dir_page_->SetBucketPageId(i, new_bucket_page_id);
    //     dir_page_->IncrLocalDepth(i);
    //   }
    // }

    // 与上面代码实现一样的功能
    for (uint32_t i = new_local_hash; i < dir_size; i += new_local_mask + 1) {
      dir_page_->SetBucketPageId(i, new_bucket_page_id);
      dir_page_->IncrLocalDepth(i);
    }
  } else {  // 目录长度变成原来的两倍
    dir_page_->IncrGlobalDepth();
    uint32_t new_dir_size = dir_page_->Size();
    page_id_t upper_page_id;
    uint32_t upper_local_depth;

    // 下半部与上半部互成镜像，只是分裂的桶需修改page_id，其他的与上半部保持一致
    for (uint32_t i = dir_size; i < new_dir_size; i++) {
      upper_page_id = dir_page_->GetBucketPageId(i - dir_size);
      upper_local_depth = dir_page_->GetLocalDepth(i - dir_size);
      if (upper_page_id == old_bucket_page_id) {  // 分裂桶对应的桶
        dir_page_->SetBucketPageId(i, new_bucket_page_id);
      } else {  // 其余桶，page id和depth与上半部保持一致
        dir_page_->SetBucketPageId(i, upper_page_id);
      }
      dir_page_->SetLocalDepth(i, upper_local_depth);  // 统一设置成与上半部一样的深度
    }
  }
  uint32_t bucket_size = old_bucket_page->Size();
  KeyType bucket_key;
  ValueType bucket_value;
  page_id_t bucket_page_id;
  // 遍历旧桶中的元素，插入部分元素至新桶
  for (uint32_t i = 0; i < bucket_size; i++) {
    bucket_key = old_bucket_page->KeyAt(i);
    bucket_page_id = KeyToPageId(bucket_key, dir_page_);
    if (bucket_page_id == new_bucket_page_id) {
      bucket_value = old_bucket_page->ValueAt(i);
      old_bucket_page->RemoveAt(i);
      new_bucket_page->Insert(bucket_key, bucket_value, comparator_);
    }
  }

  // 进行正常插入操作
  bucket_page_id = KeyToPageId(key, dir_page_);
  if (bucket_page_id == old_bucket_page_id) {
    ret = old_bucket_page->Insert(key, value, comparator_);
  } else {
    ret = new_bucket_page->Insert(key, value, comparator_);
  }
  buffer_pool_manager_->UnpinPage(old_bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true, nullptr);
  table_latch_.WUnlock();
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page_);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool ret = bucket_page->Remove(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);  // 要提前unpin，有可能要删除该桶
  table_latch_.RUnlock();
  // 若当前桶为空，需要进行合并操作，合并完之后判断是否需要循环合并
  if (ret && bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
    while (ExtraMerge(transaction, key, value)) {
    }
  }

  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  uint32_t index = KeyToDirectoryIndex(key, dir_page_);  // 索引值，例如为1011
  page_id_t bucket_page_id = KeyToPageId(key, dir_page_);
  uint32_t dir_size = dir_page_->Size();
  uint32_t local_depth = dir_page_->GetLocalDepth(index);         // 局部深度，例如为3
  uint32_t old_local_mask = dir_page_->GetLocalDepthMask(index);  // 合并前掩码，例如为111
  uint32_t new_local_mask = old_local_mask ^ (1 << (local_depth - 1));  // 合并后的掩码，将最高位的1去掉，例如为11
  uint32_t old_local_hash = index & old_local_mask;                     // 与后的值，例如为011
  uint32_t new_local_hash = index & new_local_mask;                     // 与后的值，例如为11

  auto *bucket_page = FetchBucketPage(bucket_page_id);
  bool merge_occur = false;  // 标志是否发生合并

  if (local_depth > 0 && bucket_page->IsEmpty()) {  // remove函数加的是读锁，有可能已经插入新值
    // 获取与空桶对应的桶的信息，如果两者深度一致，则可以合并成一个桶
    page_id_t another_bucket_page_id;
    uint32_t another_bucket_idx = dir_page_->GetSplitImageIndex(index);
    uint32_t another_local_depth = dir_page_->GetLocalDepth(another_bucket_idx);
    if (another_local_depth == local_depth) {  // 此时可以进行合并操作
      merge_occur = true;
      another_bucket_page_id = dir_page_->GetBucketPageId(another_bucket_idx);
    }
    if (merge_occur) {
      // for (uint32_t i = 0; i < dir_size; i++) {
      //   if ((i & old_local_mask) == (index & old_local_mask)) {  // 寻找指向空桶的指针,将其指向另一半another_bucket
      //     dir_page_->SetBucketPageId(i, another_bucket_page_id);
      //   }
      // }

      // 与上面代码实现一样的功能
      for (uint32_t i = old_local_hash; i < dir_size; i += old_local_mask + 1) {
        dir_page_->SetBucketPageId(i, another_bucket_page_id);
      }

      // for (uint32_t i = 0; i < dir_size; i++) {
      //   if ((i & new_local_mask) == (index & new_local_mask)) {  // 将所有指向another_bucket的local depth都减一
      //     dir_page_->DecrLocalDepth(i);
      //   }
      // }

      // 与上面代码实现一样的功能
      for (uint32_t i = new_local_hash; i < dir_size; i += new_local_mask + 1) {
        dir_page_->DecrLocalDepth(i);
      }
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);  // 先unpin再删除
      buffer_pool_manager_->DeletePage(bucket_page_id, nullptr);
      bool ret = dir_page_->CanShrink();
      if (ret) {  // 降低全局深度
        dir_page_->DecrGlobalDepth();
      }
    }
  }
  if (!merge_occur) {  // 合并未发生
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }
  table_latch_.WUnlock();
}

// 循环合并
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page_);
  uint32_t index = KeyToDirectoryIndex(key, dir_page_);
  uint32_t local_depth = dir_page_->GetLocalDepth(index);
  uint32_t dir_size = dir_page_->Size();
  bool extra_merge_occur = false;
  if (local_depth > 0) {
    // 计算合并后桶对应的桶的各项信息，该桶有可能因为已经为空但由于深度不一致没有合并
    auto extra_bucket_idx = dir_page_->GetSplitImageIndex(index);
    auto extra_local_depth = dir_page_->GetLocalDepth(extra_bucket_idx);
    auto extra_bucket_page_id = dir_page_->GetBucketPageId(extra_bucket_idx);
    auto *extra_bucket = FetchBucketPage(extra_bucket_page_id);
    if (extra_local_depth == local_depth && extra_bucket->IsEmpty()) {  // 进行合并操作
      extra_merge_occur = true;

      uint32_t old_local_mask = dir_page_->GetLocalDepthMask(extra_bucket_idx);
      uint32_t new_local_mask = old_local_mask ^ (1 << (local_depth - 1));
      uint32_t old_local_hash = extra_bucket_idx & old_local_mask;
      uint32_t new_local_hash = extra_bucket_idx & new_local_mask;
      for (uint32_t i = old_local_hash; i < dir_size; i += old_local_mask + 1) {
        dir_page_->SetBucketPageId(i, bucket_page_id);
      }
      for (uint32_t i = new_local_hash; i < dir_size; i += new_local_mask + 1) {
        dir_page_->DecrLocalDepth(i);
      }
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);  // 先unpin再删除
      buffer_pool_manager_->DeletePage(extra_bucket_page_id, nullptr);

      bool ret = dir_page_->CanShrink();
      if (ret) {  // 降低全局深度
        dir_page_->DecrGlobalDepth();
      }
    }
    if (!extra_merge_occur) {  // 循环合并未发生
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);
    }
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.WUnlock();
  return extra_merge_occur;
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
