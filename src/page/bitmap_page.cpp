#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ >= 8*MAX_CHARS)
  return false;
  else
  {
    page_allocated_++;

    next_free_page_ = 0;
    while(!IsPageFree(next_free_page_)&&next_free_page_<8*MAX_CHARS)
    {
      next_free_page_++;
    }
    page_offset = next_free_page_;
    bytes[next_free_page_/8] |= (0x80 >> (page_offset % 8));
    return true;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(!IsPageFreeLow(page_offset/8, page_offset%8))
  {
    page_allocated_--;
    bytes[page_offset/8] &= ~(0x80 >> (page_offset % 8));
    return true;
  }
  else
  {
    //std::cout<<"error in DeAllocatePage!"<<std::endl;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset/8, page_offset%8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if(bytes[byte_index] & (0x80 >> (bit_index)))
  return false;

  return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;