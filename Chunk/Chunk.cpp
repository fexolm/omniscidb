/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @file Chunk.cpp
 * @author Wei Hong <wei@mapd.com>
 */

#include "Chunk.h"
#include <ittnotify.h>
#include "../DataMgr/ArrayNoneEncoder.h"
#include "../DataMgr/FixedLengthArrayNoneEncoder.h"
#include "../DataMgr/StringNoneEncoder.h"

__itt_domain* domain = __itt_domain_create("Traces.Chunk");
__itt_string_handle* getChunkBufferTask =
    __itt_string_handle_create("Traces.Chunk.getChunkBuffer");
__itt_string_handle* createChunkBufferTask =
    __itt_string_handle_create("Traces.Chunk.createChunkBuffer");
__itt_string_handle* getNumElemsForBytesInsertDataTask =
    __itt_string_handle_create("Traces.Chunk.getNumElemsForBytesInsertData");
__itt_string_handle* appendDataTask =
    __itt_string_handle_create("Traces.Chunk.appendData");

namespace Chunk_NS {
std::shared_ptr<Chunk> Chunk::getChunk(const ColumnDescriptor* cd,
                                       DataMgr* data_mgr,
                                       const ChunkKey& key,
                                       const MemoryLevel memoryLevel,
                                       const int deviceId,
                                       const size_t numBytes,
                                       const size_t numElems) {
  std::shared_ptr<Chunk> chunkp = std::make_shared<Chunk>(Chunk(cd));
  chunkp->getChunkBuffer(data_mgr, key, memoryLevel, deviceId, numBytes, numElems);
  return chunkp;
}

bool Chunk::isChunkOnDevice(DataMgr* data_mgr,
                            const ChunkKey& key,
                            const MemoryLevel mem_level,
                            const int device_id) {
  if (column_desc->columnType.is_varlen() && !column_desc->columnType.is_fixlen_array()) {
    ChunkKey subKey = key;
    ChunkKey indexKey(subKey);
    indexKey.push_back(1);
    ChunkKey dataKey(subKey);
    dataKey.push_back(2);
    return data_mgr->isBufferOnDevice(indexKey, mem_level, device_id) &&
           data_mgr->isBufferOnDevice(dataKey, mem_level, device_id);
  } else {
    return data_mgr->isBufferOnDevice(key, mem_level, device_id);
  }
}

void Chunk::getChunkBuffer(DataMgr* data_mgr,
                           const ChunkKey& key,
                           const MemoryLevel mem_level,
                           const int device_id,
                           const size_t num_bytes,
                           const size_t num_elems) {
  __itt_task_begin(domain, __itt_null, __itt_null, getChunkBufferTask);
  if (column_desc->columnType.is_varlen() && !column_desc->columnType.is_fixlen_array()) {
    ChunkKey subKey = key;
    subKey.push_back(1);  // 1 for the main buffer
    buffer = data_mgr->getChunkBuffer(subKey, mem_level, device_id, num_bytes);
    subKey.pop_back();
    subKey.push_back(2);  // 2 for the index buffer
    index_buf = data_mgr->getChunkBuffer(
        subKey,
        mem_level,
        device_id,
        (num_elems + 1) * sizeof(StringOffsetT));  // always record n+1 offsets so string
                                                   // length can be calculated
    switch (column_desc->columnType.get_type()) {
      case kARRAY: {
        ArrayNoneEncoder* array_encoder =
            dynamic_cast<ArrayNoneEncoder*>(buffer->encoder.get());
        array_encoder->set_index_buf(index_buf);
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        CHECK_EQ(kENCODING_NONE, column_desc->columnType.get_compression());
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        str_encoder->set_index_buf(index_buf);
        break;
      }
      case kPOINT:
      case kLINESTRING:
      case kPOLYGON:
      case kMULTIPOLYGON: {
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        str_encoder->set_index_buf(index_buf);
        break;
      }
      default:
        CHECK(false);
    }
  } else {
    buffer = data_mgr->getChunkBuffer(key, mem_level, device_id, num_bytes);
  }
  __itt_task_end(domain);
}

void Chunk::createChunkBuffer(DataMgr* data_mgr,
                              const ChunkKey& key,
                              const MemoryLevel mem_level,
                              const int device_id,
                              const size_t page_size) {
  __itt_task_begin(domain, __itt_null, __itt_null, createChunkBufferTask);
  if (column_desc->columnType.is_varlen() && !column_desc->columnType.is_fixlen_array()) {
    ChunkKey subKey = key;
    subKey.push_back(1);  // 1 for the main buffer
    buffer = data_mgr->createChunkBuffer(subKey, mem_level, device_id, page_size);
    subKey.pop_back();
    subKey.push_back(2);  // 2 for the index buffer
    index_buf = data_mgr->createChunkBuffer(subKey, mem_level, device_id, page_size);
  } else {
    buffer = data_mgr->createChunkBuffer(key, mem_level, device_id, page_size);
  }
  __itt_end_task(domain);
}

size_t Chunk::getNumElemsForBytesInsertData(const DataBlockPtr& src_data,
                                            const size_t num_elems,
                                            const size_t start_idx,
                                            const size_t byte_limit,
                                            const bool replicating) {
  __itt_task_begin(domain, __itt_null, __itt_null, getNumElemsForBytesInsertDataTask);

  CHECK(column_desc->columnType.is_varlen());
  switch (column_desc->columnType.get_type()) {
    case kARRAY: {
      if (column_desc->columnType.get_size() > 0) {
        FixedLengthArrayNoneEncoder* array_encoder =
            dynamic_cast<FixedLengthArrayNoneEncoder*>(buffer->encoder.get());
        return array_encoder->getNumElemsForBytesInsertData(
            src_data.arraysPtr, start_idx, num_elems, byte_limit, replicating);
      }
      ArrayNoneEncoder* array_encoder =
          dynamic_cast<ArrayNoneEncoder*>(buffer->encoder.get());
      return array_encoder->getNumElemsForBytesInsertData(
          src_data.arraysPtr, start_idx, num_elems, byte_limit, replicating);
    }
    case kTEXT:
    case kVARCHAR:
    case kCHAR: {
      CHECK_EQ(kENCODING_NONE, column_desc->columnType.get_compression());
      StringNoneEncoder* str_encoder =
          dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
      return str_encoder->getNumElemsForBytesInsertData(
          src_data.stringsPtr, start_idx, num_elems, byte_limit, replicating);
    }
    case kPOINT:
    case kLINESTRING:
    case kPOLYGON:
    case kMULTIPOLYGON: {
      StringNoneEncoder* str_encoder =
          dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
      return str_encoder->getNumElemsForBytesInsertData(
          src_data.stringsPtr, start_idx, num_elems, byte_limit, replicating);
    }
    default:
      CHECK(false);
      return 0;
  }
  __itt_end_task(domain);
}

ChunkMetadata Chunk::appendData(DataBlockPtr& src_data,
                                const size_t num_elems,
                                const size_t start_idx,
                                const bool replicating) {
  __itt_task_begin(domain, __itt_null, __itt_null, appendData);

  const auto& ti = column_desc->columnType;
  if (ti.is_varlen()) {
    switch (ti.get_type()) {
      case kARRAY: {
        if (ti.get_size() > 0) {
          FixedLengthArrayNoneEncoder* array_encoder =
              dynamic_cast<FixedLengthArrayNoneEncoder*>(buffer->encoder.get());
          return array_encoder->appendData(
              src_data.arraysPtr, start_idx, num_elems, replicating);
        }
        ArrayNoneEncoder* array_encoder =
            dynamic_cast<ArrayNoneEncoder*>(buffer->encoder.get());
        return array_encoder->appendData(
            src_data.arraysPtr, start_idx, num_elems, replicating);
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        CHECK_EQ(kENCODING_NONE, ti.get_compression());
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        return str_encoder->appendData(
            src_data.stringsPtr, start_idx, num_elems, replicating);
      }
      case kPOINT:
      case kLINESTRING:
      case kPOLYGON:
      case kMULTIPOLYGON: {
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        return str_encoder->appendData(
            src_data.stringsPtr, start_idx, num_elems, replicating);
      }
      default:
        CHECK(false);
    }
  }
  __itt_end_task(domain);
  return buffer->encoder->appendData(src_data.numbersPtr, num_elems, ti, replicating);
}

void Chunk::unpin_buffer() {
  if (buffer != nullptr) {
    buffer->unPin();
  }
  if (index_buf != nullptr) {
    index_buf->unPin();
  }
}

void Chunk::init_encoder() {
  buffer->initEncoder(column_desc->columnType);
  if (column_desc->columnType.is_varlen() && !column_desc->columnType.is_fixlen_array()) {
    switch (column_desc->columnType.get_type()) {
      case kARRAY: {
        ArrayNoneEncoder* array_encoder =
            dynamic_cast<ArrayNoneEncoder*>(buffer->encoder.get());
        array_encoder->set_index_buf(index_buf);
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        CHECK_EQ(kENCODING_NONE, column_desc->columnType.get_compression());
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        str_encoder->set_index_buf(index_buf);
        break;
      }
      case kPOINT:
      case kLINESTRING:
      case kPOLYGON:
      case kMULTIPOLYGON: {
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer->encoder.get());
        str_encoder->set_index_buf(index_buf);
        break;
      }
      default:
        CHECK(false);
    }
  }
}

ChunkIter Chunk::begin_iterator(const ChunkMetadata& chunk_metadata,
                                int start_idx,
                                int skip) const {
  ChunkIter it;
  it.type_info = column_desc->columnType;
  it.skip = skip;
  it.skip_size = column_desc->columnType.get_size();
  if (it.skip_size < 0) {  // if it's variable length
    it.current_pos = it.start_pos =
        index_buf->getMemoryPtr() + start_idx * sizeof(StringOffsetT);
    it.end_pos = index_buf->getMemoryPtr() + index_buf->size() - sizeof(StringOffsetT);
    it.second_buf = buffer->getMemoryPtr();
  } else {
    it.current_pos = it.start_pos = buffer->getMemoryPtr() + start_idx * it.skip_size;
    it.end_pos = buffer->getMemoryPtr() + buffer->size();
    it.second_buf = nullptr;
  }
  it.num_elems = chunk_metadata.numElements;
  return it;
}
}  // namespace Chunk_NS
