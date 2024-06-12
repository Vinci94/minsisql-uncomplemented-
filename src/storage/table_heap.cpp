#include "storage/table_heap.h"
#include "glog/logging.h"
/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    ASSERT(false, "Page not found");
    return false;
  }

  // Step2: Insert the tuple into the page.
  while (true) {
    page->WLatch();
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
    if (page->GetNextPageId() != INVALID_PAGE_ID) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    } 
    else 
    {
      page_id_t new_page_id;
      buffer_pool_manager_->NewPage(new_page_id);
      page->SetNextPageId(new_page_id);
      new_page_id = page->GetPageId();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      page->WLatch();
      page->Init(page->GetPageId(), new_page_id /*it actually is previous page's page_id */, log_manager_, txn);
      if(!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
      {
        page->WUnlatch();
        return false;
      }
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
  }

  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Update the tuple in the page.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  int result_type;
  if (page == nullptr) {
    ASSERT(false, "Page not found");
    return false;
  }
  Row old_row(rid);
  page->RLatch();
  if (GetTuple(&old_row, txn) == false) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  page->WLatch();
  result_type = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  if (result_type == 0) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else if (result_type == 3) {
    page->WLatch();
    if (MarkDelete(rid, txn) && InsertTuple(const_cast<Row &>(row), txn)) {
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page->WUnlatch();
      return true;
    }
    page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    ASSERT(false, "Page not found");
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  ASSERT(page != nullptr, "Can't find page!");

  page->RLatch();
  if (!page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    ASSERT(false, "Page not found");
    return TableIterator(nullptr, INVALID_ROWID, nullptr);
  }
  page->RLatch();
  RowId rid;
  page->GetFirstTupleRid(&rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return TableIterator(this, rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(this, INVALID_ROWID, nullptr); }
