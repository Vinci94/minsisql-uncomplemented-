#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), txn_(txn) 
{
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    row_ = new Row(rid);
    table_heap_->GetTuple(row_, txn_);
  } else {
    row_ = new Row(INVALID_ROWID);
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  this->table_heap_ = other.table_heap_;
  this->txn_ = new Txn;
  this->row_ = new Row(*(other.row_));
}

TableIterator::~TableIterator() {
  delete txn_;
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->table_heap_ == itr.table_heap_ && row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() { return *row_; }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->table_heap_ = itr.table_heap_;
  this->txn_ = new Txn;
  this->row_ = new Row(*(itr.row_));
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // if row_ is nullptr or row_ is the last row in the page
  if (row_ == nullptr || row_->GetRowId() == INVALID_ROWID) {
    delete row_;
    row_ = new Row(INVALID_ROWID);
    return *this;
  }
  // get the next row in the page
  RowId next_rid;
  TablePage *page =
      reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  page->RLatch();
  if (page->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
    delete row_;
    row_ = new Row(next_rid);
    table_heap_->GetTuple(row_, txn_);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  } else {
    // if the next row is not in the page, get the next page
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next_page_id = page->GetNextPageId();
    while (next_page_id != INVALID_PAGE_ID) {
      auto new_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page->RUnlatch();
      page = new_page;
      page->RLatch();
      if (page->GetFirstTupleRid(&next_rid)) {
        delete row_;
        row_ = new Row(next_rid);
        table_heap_->GetTuple(row_, txn_);
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return *this;
      } else {
        next_page_id = page->GetNextPageId();
      }
    }
    // if there is no next row, set row_ to nullptr
    delete row_;
    row_ = new Row(INVALID_ROWID);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  }
}

// iter++
 TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++(*this);
  return TableIterator(temp);
}
