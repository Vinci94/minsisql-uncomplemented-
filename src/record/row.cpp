#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t map_offset = 0,field_offset = 0 ,field_num = fields_.size(),map_size = (field_num + 7) / 8;

  MACH_WRITE_UINT32(buf, field_num);
  buf += 4;

  for(uint32_t i = 0;i<map_size;i++){
    uint8_t map = 0;
    for(u_int8_t j = 0;j<8;j++){
      if(i*8+j >= field_num){
        break;
      }
      if(fields_[i*8+j]->IsNull()){
        map |= (1 << j);
      }
      else
      {
        map &= ~(1 << j);
        field_offset += fields_[i*8+j]->SerializeTo(buf+map_size+field_offset);
      }
    }
    buf[map_offset++] = map;
  }
  return 4+map_offset+field_offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  
  uint32_t field_num = MACH_READ_UINT32(buf);
  buf += 4;

  uint32_t map_offset = 0,field_offset = 0,map_size = (field_num + 7) / 8;
  Field* f;

  for(uint32_t i = 0;i<map_size;i++){
    uint8_t map = buf[map_offset++];
    for(u_int8_t j = 0;j<8;j++){
      if(i*8+j >= field_num){
        break;
      }
      TypeId type = schema->GetColumn(i*8+j)->GetType();
      switch(type)
      {
        case TypeId::kTypeInt:
          f = new Field(type,0);
          break;
        case TypeId::kTypeFloat:
          f = new Field(type,0.0f);
          break;
        case TypeId::kTypeChar:
          f = new Field(type,nullptr,0,false);
          break;
        default:
          ASSERT(false, "Unknown field type.");
          break;
      }
      if(map & (1 << j)){
        field_offset += f->DeserializeFrom(buf+map_size+field_offset,type,&f,true);
      }
      else
      {
        field_offset += f->DeserializeFrom(buf+map_size+field_offset,type,&f,false);
      }
      fields_.emplace_back(f);
    }
  }
  return 4+map_offset+field_offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t offsize = (fields_.size() + 7) / 8+4;//map_size+field_num(4 bytes)

  //calculate the size of fields
  for(auto& field : fields_){
    offsize += field->GetSerializedSize();
  }

  return offsize;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
