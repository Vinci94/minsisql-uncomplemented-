#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
{
  lru_list_.clear();
  Table.clear();
  Table.reserve(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(Size())
  {
    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    Table.erase(*frame_id);
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
if(Table.find(frame_id)!=Table.end())
  {
    lru_list_.erase(Table[frame_id]);
    Table.erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(Table.find(frame_id)!=Table.end())
  {
    //lru_list_.erase(Table[frame_id]);
  }
  else
  {
    lru_list_.emplace_front(frame_id);
    Table[frame_id] = lru_list_.begin();
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}