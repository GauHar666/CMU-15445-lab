//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  start_index_ = 0;
  instances_.resize(num_instances);
  for (size_t i = 0; i < num_instances; i++) {
    instances_[i] = std::make_shared<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  //把所有实例都乘起来。
  return pool_size_ * num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  //对其取余，就知道应该放在哪个实例里面了。get()获取shareptr内部的指针。
  return instances_[page_id % num_instances_].get();
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  //对于newpgimp有点不一样，要根据start_instance的位置来判断新的page要放在哪个instance下面
  // Page *page;
  // size_t index = start_index_;
  // do {
  //   page = instances_[index]->NewPage(page_id);
  //   if (page != nullptr) {
  //     break;
  //   }
  //   index = (index + 1) % num_instances_;
  // } while (index != start_index_);
  // start_index_ = (start_index_ + 1) % num_instances_;
  // return page;
  //对于newpgimp有点不一样，要根据start_instance的位置来判断新的page要放在哪个instance下面
  Page *page;
  auto index = start_index_;
  //注意要判断在这个instance中会不会创建失败，如果创建失败就到下一个instance中去创建
  for(int i=index;i<=2*index;i++){
    page=instances_[i]->NewPage(page_id);
    if(page==nullptr){
      break;
    }
    index = (index+1)%num_instances_;
  }
  start_index_ = (start_index_+1)%num_instances_;
  return page;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->DeletePage(page_id);
  return false;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances，对每个instance里面执行FlushAllPages。
  for (auto &instance : instances_) {
    instance->FlushAllPages();
  }
}

}  // namespace bustub
