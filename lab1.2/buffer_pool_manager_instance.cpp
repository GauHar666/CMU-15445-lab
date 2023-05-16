//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // 初始化pages数组和LRUreplacer
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // 初始化，让每一页加入到free_list中。
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

//析构：主要释放pages和replacer
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // // Make sure you call DiskManager::WritePage!
  // std::lock_guard<std::mutex> lock(latch_);  // 加锁
  // if (page_table_.count(page_id) == 0) {
  //   return false;
  // }

  // frame_id_t frame_id = page_table_[page_id];
  // /*
  // if (pages_[frame_id].IsDirty()) {
  //   disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  //   pages_[frame_id].is_dirty_ = false;
  // }
  // 我本来以为只有dirty时才进行页的写入，但parallel_buffer_pool_manager_test.cpp 942-944行的逻辑表明不是这样的
  // strcpy(page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
  // // FLush page instead of unpining with true
  // EXPECT_EQ(1, bpm->FlushPage(temp_page_id, nullptr));
  // EXPECT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));
  // */
  // disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  // pages_[frame_id].is_dirty_ = false;
  // return true;
  std::lock_guard<std::mutex> lock(latch_);
  if(page_table_.count(page_id)==0){
    return false;
  }
  frame_id_t frame_id=page_table_[page_id];
  disk_manager_->WritePage(page_id,pages_[frame_id].data_);
  pages_[frame_id].is_dirty_=false;
  return true;
}

//让每一页刷新。
void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  page_id_t page_id;
  frame_id_t frame_id;
  std::lock_guard<std::mutex> lock(latch_);  // 加锁
  for (const auto &item : page_table_) {
    page_id = item.first;
    frame_id = item.second;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
}

frame_id_t BufferPoolManagerInstance::GetFrame() {
  frame_id_t frame_id;
  if (!free_list_.empty()) {  // 存在空余页
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {  // 需根据LRU算法淘汰一页
    bool res = replacer_->Victim(&frame_id);
    if (!res) {  // 淘汰失败
      return NUMLL_FRAME;
    }
    page_id_t victim_page_id = pages_[frame_id].page_id_;
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(victim_page_id, pages_[frame_id].data_);
    }
    page_table_.erase(victim_page_id);  // 在page_table中删除该frame对应的页
  }
  return frame_id;
}
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  page_id_t new_page_id;
  std::lock_guard<std::mutex> lock(latch_);  // 加锁
  frame_id = GetFrame();
  if (frame_id == NUMLL_FRAME) {
    return nullptr;
  }
  new_page_id = AllocatePage();//初始化分配内存功能，在下面实现
  //把page_table_里面对应的frame_id更新
  page_table_[new_page_id] = frame_id;

  //更新pages_
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory(); 
  /*
  创建新页也需要写回磁盘，如果不这样 newpage unpin 然后再被淘汰出去 fetchpage时就会报错（磁盘中并无此页）
  但不能直接is_dirty_置为true，测试会报错
 */
  disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id;
  std::lock_guard<std::mutex> lock(latch_);  // 加锁
  if (page_table_.count(page_id) > 0) {      // 原先就在buffer里
    frame_id = page_table_[page_id];
    //那么要判断他是不是unpin的，如果是的话，你就可以把它置换出来换成你想要的新页。
    // 只有pin_count为0才有可能在replacer里，这个时候可以替换出去，调用Pin方法。
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->Pin(frame_id);
    }
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  //如果不在，那么要拿一个新的frame来放。
  frame_id = GetFrame();
  if (frame_id == NUMLL_FRAME) {
    return nullptr;
  }
  page_table_[page_id] = frame_id;

  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);//这里要记得读一下page，把page的内容读到disk
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  frame_id_t frame_id;
  std::lock_guard<std::mutex> lock(latch_);  // 加锁

  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id = page_table_[page_id];
  Page &delete_page = pages_[frame_id];
  if (delete_page.pin_count_ != 0) {
    return false;
  }
  // 不需要写回页，该页已删除
  // if (delete_page.IsDirty()) {
  //   disk_manager_->WritePage(page_id, delete_page.data_);
  // }
  // 从页表中删除该页，并将页框放回空闲列表
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);
  free_list_.emplace_back(frame_id);

  delete_page.page_id_ = INVALID_PAGE_ID;
  delete_page.pin_count_ = 0;
  delete_page.is_dirty_ = false;
  DeallocatePage(page_id);  // 调用DeallocatePage方法释放内存
  return true;
}

// 放回页
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }
  if (is_dirty) {  // 不能直接赋值
    page.is_dirty_ = true;
  }
  page.pin_count_--;
  if (page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;//分配下一页。
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
