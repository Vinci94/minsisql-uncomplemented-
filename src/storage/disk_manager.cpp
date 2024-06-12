#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
//meta_data_ contains num_allocated_pages_ and num_extents_ 
//and page_allocated_ of whole(1024) bitmaps
page_id_t DiskManager::AllocatePage() {

  if(reinterpret_cast<DiskFileMetaPage*>(meta_data_)->num_allocated_pages_ >= MAX_VALID_PAGE_ID) {
    ASSERT(false, "pages are overflow.");
    return INVALID_PAGE_ID;
  }
  else
  {
    uint32_t meta_data_uint[PAGE_SIZE/4],free_page_id;
    page_id_t page_logic_id, bitmap_phys_id;
    uint32_t extent_id = 0;
    char bitmap[PAGE_SIZE];

    memcpy(meta_data_uint, meta_data_, PAGE_SIZE);

    //find the first extent that is not full
    while(*(meta_data_uint + 2 + extent_id) == BITMAP_SIZE)
    {
      extent_id++;
    }

    //find the first free page in the extent
    bitmap_phys_id = extent_id * (BITMAP_SIZE + 1) + 1;
    ReadPhysicalPage(bitmap_phys_id, bitmap);
    BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
    bitmap_page->AllocatePage(free_page_id);
    page_logic_id = extent_id * BITMAP_SIZE + free_page_id;

    //update the meta_data_
    meta_data_uint[0]++;
    *(meta_data_uint + 2 + extent_id) += 1;
    if(extent_id == meta_data_uint[1])
    {
      meta_data_uint[1]++;
    }
    else if(extent_id > meta_data_uint[1])
    {
      ASSERT(false, "extent_id is larger than num_extents_");
    }

    //write back
    memcpy(meta_data_, meta_data_uint, PAGE_SIZE);
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    WritePhysicalPage(bitmap_phys_id, bitmap);
    return page_logic_id;
  }
  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  char bitmap[PAGE_SIZE];
  page_id_t bitmap_phy_id = logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1) + 1;
  
  //find the bitmap page
  ReadPhysicalPage(bitmap_phy_id, bitmap);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  if(!(bitmap_page->DeAllocatePage(logical_page_id%BITMAP_SIZE)))
    ASSERT(false, "Deallocate page failed.");
  
  //update the meta_data_
  reinterpret_cast<DiskFileMetaPage*>(meta_data_)->num_allocated_pages_--;
  reinterpret_cast<DiskFileMetaPage*>(meta_data_)->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_phy_id, bitmap);
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bitmap[PAGE_SIZE];
  page_id_t bitmap_phy_id = logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(bitmap_phy_id, bitmap);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  return bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return 2 + logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1) + logical_page_id % BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}