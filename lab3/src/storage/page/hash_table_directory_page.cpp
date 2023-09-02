//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
auto HashTableDirectoryPage::GetPageId() const -> page_id_t { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

auto HashTableDirectoryPage::GetLSN() const -> lsn_t { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

auto HashTableDirectoryPage::GetGlobalDepth() -> uint32_t { return global_depth_; }

auto HashTableDirectoryPage::GetGlobalDepthMask() -> uint32_t { return (1 << global_depth_) - 1; }

void HashTableDirectoryPage::IncrGlobalDepth() { global_depth_++; }

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) -> page_id_t { return bucket_page_ids_[bucket_idx]; }

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto HashTableDirectoryPage::Size() -> uint32_t { return 1 << global_depth_; }

auto HashTableDirectoryPage::CanShrink() -> bool {  // 判断是不是所有local depth都小于global depth
  uint32_t dir_size = 1 << global_depth_;
  for (uint32_t index = 0; index < dir_size; index++) {
    if (local_depths_[index] == global_depth_) {
      return false;
    }
  }
  return true;
}
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {  // 得到与该桶对应的桶，即将该桶最高位置反
  uint32_t local_depth = GetLocalDepth(bucket_idx);
  uint32_t local_mask = GetLocalDepthMask(bucket_idx);
  if (local_depth == 0) {
    return 0;
  }
  return (bucket_idx ^ (1 << (local_depth - 1))) & local_mask;
  // 假设只有两个桶 0 1，深度皆为1，则0 ^ (1<<(1-1)) = 1
  // 假设桶11深度为1,，则其实际上用到的位为1，对应的桶即为0  11 ^ 1 & 1 = 0
  // 但实际上返回10也无所谓，因为二者深度相等才进行合并操作
  // 当只有一个桶时返回本身
}
auto HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) -> uint32_t { return local_depths_[bucket_idx]; }

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
  uint32_t local_depth = GetLocalDepth(bucket_idx);
  return (1 << local_depth) - 1;
}
void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

auto HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) -> uint32_t { return 0; }

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_count, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
