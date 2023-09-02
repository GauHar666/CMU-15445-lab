//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {
// 我认为容量是没有用的，因为replacer的大小和pages大小一样，不可能超过
LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(mutex_);
  if (lru_list_.empty()) {
    return false;
  }

  // 淘汰LRU列表末尾页框，同时删除辅助map中对应映射
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  speed_map_.erase(*frame_id);
  return true;
}

// 取出
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (speed_map_.count(frame_id) > 0) {
    lru_list_.erase(speed_map_[frame_id]);
    speed_map_.erase(frame_id);
  }
}

// 放回
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 对同一个元素调用两次unpin函数，第二次无效
  if (speed_map_.count(frame_id) == 0) {
    lru_list_.emplace_front(frame_id);
    speed_map_.insert({frame_id, lru_list_.begin()});
  }
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(mutex_);
  return lru_list_.size();
}

}  // namespace bustub
