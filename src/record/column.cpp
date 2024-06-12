#include "record/column.h"
#include <cstring>
#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
  // 1.magic_num 4 2.name长度 4 3.name实际数据 name.length() 4.type_id(enum类型是int) 4
  // 5.len_ 4      6.table_ind_ 4  7.unique_ 1
*/
uint32_t Column::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, name_.size());
  buf += 4;
  MACH_WRITE_STRING(buf, name_);
  buf += name_.length();
  MACH_WRITE_UINT32(buf, type_);
  buf += 4;
  MACH_WRITE_UINT32(buf, len_);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_ind_);
  buf += 4;
  MACH_WRITE_TO(bool, buf, unique_);
  buf++;

  return 21 + name_.length();
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return 21 + name_.length();
  
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t magic_num, name_len, len, table_ind;
  std::string name;
  TypeId type;
  bool unique;
  magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(ERROR) << "magic_num error";
    return 0;
  }

  name_len = MACH_READ_UINT32(buf);
  buf += 4;

  name = std::string(buf, name_len);
  buf += name_len;

  type = static_cast<TypeId>(MACH_READ_UINT32(buf));
  buf += 4;
  len = MACH_READ_UINT32(buf);
  buf += 4;
  table_ind = MACH_READ_UINT32(buf);
  buf += 4;

  unique = MACH_READ_FROM(bool, buf);
  buf++;
  column = new Column(name, type, len, table_ind, false, unique);
  return 21+name_len;
}
