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

//析构也不需要释放相应内存。
LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    //先上锁保证线程安全
    std::lock_guard<std::mutex> lock(mutex_);
    if(lru_list_.empty()){
        return false;
    } 

    //如果不为空则删除链表最后面的元素，链表是从新---》旧的。
    //拿到的是指针变量，所以需要解引用
    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    speed_map_.erase(*frame_id);
    return true;
}

//去除某个指定的页
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(speed_map_.count(frame_id)>0){
        lru_list_.erase(speed_map_[frame_id]);//y要把这个节点拿掉
        speed_map_.erase(frame_id);
    }
}

//放回指定的页
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(speed_map_.count(frame_id)==0){
        lru_list_.push_front(frame_id);
        speed_map_.insert({frame_id,lru_list_.begin()});
    }
}

auto LRUReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> lock(mutex_); 
    //return speed_map_.size();
    return lru_list_.size(); 
}

}  // namespace bustub
