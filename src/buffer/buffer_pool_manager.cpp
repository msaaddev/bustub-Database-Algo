//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  frame_id_t temp = 0;
  std::unordered_map<page_id_t, frame_id_t>::const_iterator result = page_table_.find(page_id);
  if (result != page_table_.end()) {
    temp = page_table_[page_id];
    replacer_->Pin(temp);
    return &pages_[temp];
  }

  if (free_list_.empty()) {
    if (replacer_->Victim(&page_id)) {
      Page *tempPage = &pages_[temp];
      if (tempPage->IsDirty()) {
        FlushPageImpl(tempPage->page_id_);
        tempPage->is_dirty_ = false;
      }

      page_table_.erase(tempPage->page_id_);
      page_table_.insert({page_id, temp});

    } else {
      return nullptr;
    }

  } else {
    temp = free_list_.front();
    free_list_.pop_front();
  }

  pages_[temp].page_id_ = page_id;
  pages_[temp].pin_count_ = 1;
  replacer_->Pin(temp);

  pages_[temp].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[temp].GetData());

  return &pages_[temp];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unordered_map<page_id_t, frame_id_t>::const_iterator result = page_table_.find(page_id);
  if (result != page_table_.end()) {
    frame_id_t temp = page_table_[page_id];
    Page *unPinPage = &pages_[temp];

    if (unPinPage->pin_count_ < 0) {
      return false;
    } else if (unPinPage->pin_count_ == 0) {
      replacer_->Unpin(temp);
    }

    unPinPage->pin_count_ -= 1;
    if (is_dirty) {
      unPinPage->is_dirty_ = true;
    }
    return true;
  } else {
    return false;
  }
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unordered_map<page_id_t, frame_id_t>::const_iterator result = page_table_.find(page_id);
  if (result != page_table_.end()) {
    frame_id_t temp = page_table_[page_id];
    Page *flushPage = &pages_[temp];

    disk_manager_->WritePage(flushPage->page_id_, flushPage->GetData());
    return true;

  } else {
    return false;
  }
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t temp = 0;
  if (free_list_.empty()) {
    if (replacer_->Victim(&temp)) {
      Page *tempPage = &pages_[temp];
      /*  if (tempPage->is_dirty_) {
         FlushPage(tempPage->page_id_);
         tempPage->is_dirty_ = false;
       } */

      page_table_.erase(tempPage->page_id_);

    } else {
      return nullptr;
    }
  } else {
    temp = free_list_.front();
    free_list_.pop_front();
  }

  *page_id = disk_manager_->AllocatePage();
  pages_[temp].page_id_ = *page_id;
  pages_[temp].ResetMemory();
  pages_[temp].pin_count_ = 1;
  pages_[temp].is_dirty_ = false;
  replacer_->Pin(temp);
  page_table_.insert({*page_id, temp});
  return &pages_[temp];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::unordered_map<page_id_t, frame_id_t>::const_iterator result = page_table_.find(page_id);
  if (result != page_table_.end()) {
    frame_id_t temp = page_table_[page_id];
    Page *tempPage = &pages_[temp];

    if (tempPage->GetPinCount() > 0) return false;

    free_list_.remove(temp);
    page_table_.erase(tempPage->page_id_);
    tempPage->ResetMemory();

    tempPage->page_id_ = INVALID_PAGE_ID;
    tempPage->is_dirty_ = false;
    tempPage->pin_count_ = 0;
    return false;

  } else {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) FlushPageImpl(pages_[i].page_id_);
}

}  // namespace bustub
