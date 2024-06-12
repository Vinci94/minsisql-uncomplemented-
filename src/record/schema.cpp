#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {

  uint32_t offset = 0,temp;

  MACH_WRITE_UINT32(buf ,SCHEMA_MAGIC_NUM);
  offset += 4;
  buf+=4;

  MACH_WRITE_UINT32(buf,GetColumnCount());
  offset += 4;
  buf+=4;

  for(auto& column : columns_){
    temp = column->SerializeTo(buf);
    offset += temp;
    buf += temp;
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 8;
  for(auto& column : columns_){
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  uint32_t magic_num,column_count, temp;
  magic_num = MACH_READ_UINT32(buf);
  offset += 4;
  buf += 4;
  if(magic_num != SCHEMA_MAGIC_NUM){
    ASSERT(false, "Invalid schema magic number");
    return 0;
  }
  column_count = MACH_READ_UINT32(buf);
  offset += 4;
  buf += 4;

  std::vector<Column *> columns;
  for(uint32_t i = 0; i < column_count; i++){
    Column *column;
    temp = Column::DeserializeFrom(buf, column);
    columns.push_back(column);
    offset += temp;
    buf += temp;
  }
  bool is_manage = column_count?true:false;
  schema = new Schema(columns, is_manage);
  return 0;
}