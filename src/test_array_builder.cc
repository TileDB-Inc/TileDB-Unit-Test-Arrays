/**
 * @file test_array_builder.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This creates test arrays for backwards compatibility.
 *
 */

#include <tiledb/tiledb>

using namespace tiledb;

std::string encryption_key = "unittestunittestunittestunittest";

// Array Types to build
std::vector<tiledb_array_type_t> array_types = {TILEDB_DENSE, TILEDB_SPARSE};

// Dimensions valid for dense arrays
std::vector<tiledb_datatype_t> dimension_dense_datatypes = {
    TILEDB_INT8,           TILEDB_UINT8,         TILEDB_INT16,
    TILEDB_UINT16,         TILEDB_INT32,         TILEDB_UINT32,
    TILEDB_INT64,          TILEDB_UINT64,        TILEDB_DATETIME_YEAR,
    TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY,
    TILEDB_DATETIME_HR,    TILEDB_DATETIME_MIN,  TILEDB_DATETIME_SEC,
    TILEDB_DATETIME_MS,    TILEDB_DATETIME_US,   TILEDB_DATETIME_NS,
    TILEDB_DATETIME_PS,    TILEDB_DATETIME_FS,   TILEDB_DATETIME_AS};

// Dimensions valid for sparse arrays
std::vector<tiledb_datatype_t> dimension_sparse_datatypes = {
    TILEDB_INT8,           TILEDB_UINT8,         TILEDB_INT16,
    TILEDB_UINT16,         TILEDB_INT32,         TILEDB_UINT32,
    TILEDB_INT64,          TILEDB_UINT64,        TILEDB_FLOAT32,
    TILEDB_FLOAT64,        TILEDB_STRING_ASCII,  TILEDB_DATETIME_YEAR,
    TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY,
    TILEDB_DATETIME_HR,    TILEDB_DATETIME_MIN,  TILEDB_DATETIME_SEC,
    TILEDB_DATETIME_MS,    TILEDB_DATETIME_US,   TILEDB_DATETIME_NS,
    TILEDB_DATETIME_PS,    TILEDB_DATETIME_FS,   TILEDB_DATETIME_AS};

// Heterogeneous dimensions. A sparse array will be created for
// each 2-element combination.
std::vector<tiledb_datatype_t> heterogeneous_dimension_datatypes = {
    TILEDB_INT8,         TILEDB_UINT16,      TILEDB_INT32,      TILEDB_UINT64,
    TILEDB_STRING_ASCII, TILEDB_DATETIME_US, TILEDB_DATETIME_NS};

// Attribute types to check
std::vector<tiledb_datatype_t> attribute_types = {
    TILEDB_INT8,           TILEDB_UINT8,         TILEDB_INT16,
    TILEDB_UINT16,         TILEDB_INT32,         TILEDB_UINT32,
    TILEDB_INT64,          TILEDB_UINT64,        TILEDB_FLOAT32,
    TILEDB_FLOAT64,        TILEDB_CHAR,          TILEDB_STRING_ASCII,
    TILEDB_STRING_UTF8,    TILEDB_STRING_UTF16,  TILEDB_STRING_UTF32,
    TILEDB_STRING_UCS2,    TILEDB_STRING_UCS4,   TILEDB_DATETIME_YEAR,
    TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY,
    TILEDB_DATETIME_HR,    TILEDB_DATETIME_MIN,  TILEDB_DATETIME_SEC,
    TILEDB_DATETIME_MS,    TILEDB_DATETIME_US,   TILEDB_DATETIME_NS,
    TILEDB_DATETIME_PS,    TILEDB_DATETIME_FS,   TILEDB_DATETIME_AS,
    TILEDB_BLOB,           TILEDB_TIME_HR,       TILEDB_TIME_MIN,
    TILEDB_TIME_SEC,       TILEDB_TIME_MS,       TILEDB_TIME_US,
    TILEDB_TIME_NS,        TILEDB_TIME_PS,       TILEDB_TIME_FS,
    TILEDB_TIME_AS};

// Filters to test
std::vector<tiledb_filter_type_t> filters = {
    TILEDB_FILTER_NONE,
    TILEDB_FILTER_GZIP,
    TILEDB_FILTER_ZSTD,
    TILEDB_FILTER_LZ4,
    TILEDB_FILTER_RLE,
    TILEDB_FILTER_BZIP2,
    TILEDB_FILTER_DOUBLE_DELTA,
    TILEDB_FILTER_BIT_WIDTH_REDUCTION,
    TILEDB_FILTER_BITSHUFFLE,
    TILEDB_FILTER_BYTESHUFFLE,
    TILEDB_FILTER_POSITIVE_DELTA,
};

std::vector<tiledb_encryption_type_t> encryption_types = {TILEDB_NO_ENCRYPTION,
                                                          TILEDB_AES_256_GCM};

/**
 * Helper function to check of a given datatype can use the double delta filter
 * @param datatype
 * @return
 */
bool doubleDeltaCapable(tiledb_datatype_t datatype) {
  switch (datatype) {
  case TILEDB_INT8:
  case TILEDB_UINT8:
  case TILEDB_INT16:
  case TILEDB_UINT16:
  case TILEDB_INT32:
  case TILEDB_UINT32:
  case TILEDB_INT64:
  case TILEDB_UINT64:
    return true;
  default:
    return false;
  }
}

std::tuple<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>,
           std::unique_ptr<std::vector<uint8_t>>>
addDataToQuery(Query *query, std::string attributeName,
               tiledb_datatype_t datatype, bool variableLength, bool nullable);

/**
 * Create an array on disk for a given array type and dimension type. All
 * attribute types and filters will be used
 * @param ctx
 * @param array_name
 * @param array_type
 * @param dimension_1_type
 * @param dimension_2_type
 * @param encryption_type
 */
void createArray(const Context &ctx, const std::string &array_name,
                 tiledb_array_type_t array_type,
                 tiledb_datatype_t dimension_1_type,
                 tiledb_datatype_t dimension_2_type,
                 tiledb_encryption_type_t encryption_type) {
  Domain domain(ctx);

  // Build two dimensions rows and columns with domains [1,4]
  for (uint32_t i = 0; i < 2; ++i) {
    const std::string dimension_name = i == 0 ? "rows" : "cols";
    const tiledb_datatype_t dimension_type =
        i == 0 ? dimension_1_type : dimension_2_type;

    switch (dimension_type) {
    case TILEDB_INT8: {
      domain.add_dimension(
          Dimension::create<int8_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_UINT8: {
      domain.add_dimension(
          Dimension::create<uint8_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_INT16: {
      domain.add_dimension(
          Dimension::create<int16_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_UINT16: {
      domain.add_dimension(
          Dimension::create<uint16_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_INT32: {
      domain.add_dimension(
          Dimension::create<int32_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_UINT32: {
      domain.add_dimension(
          Dimension::create<uint32_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_INT64:
    case TILEDB_DATETIME_YEAR:
    case TILEDB_DATETIME_MONTH:
    case TILEDB_DATETIME_WEEK:
    case TILEDB_DATETIME_DAY:
    case TILEDB_DATETIME_HR:
    case TILEDB_DATETIME_MIN:
    case TILEDB_DATETIME_SEC:
    case TILEDB_DATETIME_MS:
    case TILEDB_DATETIME_US:
    case TILEDB_DATETIME_NS:
    case TILEDB_DATETIME_PS:
    case TILEDB_DATETIME_FS:
    case TILEDB_DATETIME_AS: {
      domain.add_dimension(
          Dimension::create<int64_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_UINT64: {
      domain.add_dimension(
          Dimension::create<uint64_t>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_FLOAT32: {
      domain.add_dimension(
          Dimension::create<float>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_FLOAT64: {
      domain.add_dimension(
          Dimension::create<double>(ctx, dimension_name, {{1, 4}}, 1));
      break;
    }
    case TILEDB_STRING_ASCII: {
      domain.add_dimension(Dimension::create(
          ctx, dimension_name, TILEDB_STRING_ASCII, nullptr, nullptr));
      break;
    }
    default: {
      assert(false);
    }
    }
  }

  // Create array schema
  ArraySchema schema(ctx, array_type);
  // Set the domain and order
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add an attribute for each data type and each filter type
  for (size_t i = 0; i < attribute_types.size(); i++) {
    for (size_t filterIndex = 0; filterIndex < filters.size(); filterIndex++) {
      tiledb_datatype_t datatype = attribute_types[i];
      tiledb_filter_type_t filter = filters[filterIndex];

      // Not all datatypes can use double delta
      if (filter == TILEDB_FILTER_DOUBLE_DELTA && !doubleDeltaCapable(datatype))
        continue;

      // Create a filter list for the attribute
      FilterList filterList(ctx);
      filterList.add_filter(Filter(ctx, filter));
      // Any only support variable length attribute
      if (datatype != TILEDB_ANY) {
        std::string attributeName = "attribute_" + impl::type_to_str(datatype) +
                                    "_" + Filter::to_str(filter);
        // Make every-other attribute nullable.
        schema.add_attribute(Attribute(ctx, attributeName, datatype)
                                 .set_filter_list(filterList)
                                 .set_nullable(i % 2 == 0));
      }

      // Create a variable length version of a given array type
      std::string varAttributeName = "attribute_variable_" +
                                     impl::type_to_str(datatype) + "_" +
                                     Filter::to_str(filter);
      Attribute varAttr = Attribute(ctx, varAttributeName, datatype);
      varAttr.set_filter_list(filterList);
      if (datatype != TILEDB_ANY) {
        varAttr.set_cell_val_num(TILEDB_VAR_NUM);
      }
      varAttr.set_nullable(i % 2 == 0);
      schema.add_attribute(varAttr);
    }
  }

  // Create the (empty) array on disk.
  if (encryption_type == TILEDB_NO_ENCRYPTION)
    Array::create(array_name, schema);
  else
    Array::create(array_name, schema, encryption_type, encryption_key.c_str(),
                  encryption_key.size() * sizeof(char));
}

/**
 * Write a single data value of 1 to the the array for all attributes and
 * dimension
 * @param ctx
 * @param array_name
 * @param dimension_1_type
 * @param dimension_2_type
 * @param encryption_type
 * @return
 */
Query::Status write_data_sparse(const Context &ctx,
                                const std::string &array_name,
                                tiledb_datatype_t dimension_1_type,
                                tiledb_datatype_t dimension_2_type,
                                tiledb_encryption_type_t encryption_type) {

  Array *array;
  if (encryption_type == TILEDB_NO_ENCRYPTION)
    array = new Array(ctx, array_name, TILEDB_WRITE);
  else
    array = new Array(ctx, array_name, TILEDB_WRITE, encryption_type,
                      encryption_key);
  Query query(ctx, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);

  // Hold a reference to the buffers so they exists until we are finished
  // writing.
  std::vector<
      std::tuple<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>,
                 std::unique_ptr<std::vector<uint8_t>>>>
      buffers;

  // Set the coordinates for the unordered write
  for (uint32_t i = 0; i < 2; ++i) {
    const std::string dimension_name = i == 0 ? "rows" : "cols";
    const tiledb_datatype_t dimension_type =
        i == 0 ? dimension_1_type : dimension_2_type;

    switch (dimension_type) {
    case TILEDB_INT8: {
      std::shared_ptr<std::vector<int8_t>> d =
          std::make_shared<std::vector<int8_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_UINT8: {
      std::shared_ptr<std::vector<uint8_t>> d =
          std::make_shared<std::vector<uint8_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_INT16: {
      std::shared_ptr<std::vector<int16_t>> d =
          std::make_shared<std::vector<int16_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_UINT16: {
      std::shared_ptr<std::vector<uint16_t>> d =
          std::make_shared<std::vector<uint16_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_INT32: {
      std::shared_ptr<std::vector<int32_t>> d =
          std::make_shared<std::vector<int32_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_UINT32: {
      std::shared_ptr<std::vector<uint32_t>> d =
          std::make_shared<std::vector<uint32_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_INT64:
    case TILEDB_DATETIME_YEAR:
    case TILEDB_DATETIME_MONTH:
    case TILEDB_DATETIME_WEEK:
    case TILEDB_DATETIME_DAY:
    case TILEDB_DATETIME_HR:
    case TILEDB_DATETIME_MIN:
    case TILEDB_DATETIME_SEC:
    case TILEDB_DATETIME_MS:
    case TILEDB_DATETIME_US:
    case TILEDB_DATETIME_NS:
    case TILEDB_DATETIME_PS:
    case TILEDB_DATETIME_FS:
    case TILEDB_DATETIME_AS:
    case TILEDB_TIME_HR:
    case TILEDB_TIME_MIN:
    case TILEDB_TIME_SEC:
    case TILEDB_TIME_MS:
    case TILEDB_TIME_US:
    case TILEDB_TIME_NS:
    case TILEDB_TIME_PS:
    case TILEDB_TIME_FS:
    case TILEDB_TIME_AS: {
      std::shared_ptr<std::vector<int64_t>> d =
          std::make_shared<std::vector<int64_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_UINT64: {
      std::shared_ptr<std::vector<uint64_t>> d =
          std::make_shared<std::vector<uint64_t>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_FLOAT32: {
      std::shared_ptr<std::vector<float>> d =
          std::make_shared<std::vector<float>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_FLOAT64: {
      std::shared_ptr<std::vector<double>> d =
          std::make_shared<std::vector<double>>();
      d->push_back(1);
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_CHAR: {
      std::shared_ptr<std::vector<char>> d =
          std::make_shared<std::vector<char>>();
      d->push_back('1');
      query.set_buffer(dimension_name, *d);
      buffers.emplace_back(nullptr, std::move(d), nullptr);
      break;
    }
    case TILEDB_STRING_ASCII: {
      std::shared_ptr<std::vector<char>> d =
          std::make_shared<std::vector<char>>();
      d->push_back('1');
      std::unique_ptr<std::vector<uint64_t>> offsets =
          std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>);
      offsets->push_back(0);

      query.set_buffer(dimension_name, *offsets, *d);
      buffers.emplace_back(std::move(offsets), std::move(d), nullptr);
      break;
    }
    default: {
      assert(false);
    }
    }
  }

  // Set the buffer for each attribute
  for (auto attribute : array->schema().attributes()) {
    auto buffer = addDataToQuery(
        &query, attribute.first, attribute.second.type(),
        attribute.second.variable_sized(), attribute.second.nullable());
    buffers.push_back(std::move(buffer));
  }
  Query::Status status = query.submit();

  // Finalize the query since we are doing unordered writes
  query.finalize();
  delete array;
  return status;
}

/**
 * Write a single data value of 1 to the the array for all attributes and
 * dimension
 * @param ctx
 * @param array_name
 * @param domain_type
 * @param encryption_type
 * @return
 */
Query::Status write_data_dense(const Context &ctx,
                               const std::string &array_name,
                               tiledb_datatype_t domain_type,
                               tiledb_encryption_type_t encryption_type) {

  Array *array;
  if (encryption_type == TILEDB_NO_ENCRYPTION)
    array = new Array(ctx, array_name, TILEDB_WRITE);
  else
    array = new Array(ctx, array_name, TILEDB_WRITE, encryption_type,
                      encryption_key);
  Query query(ctx, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);

  // Hold a reference to the buffers so they exists until we are finished
  // writing.
  std::vector<
      std::tuple<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>,
                 std::unique_ptr<std::vector<uint8_t>>>>
      buffers;

  // Set the subarray for the dense write
  switch (domain_type) {
  case TILEDB_INT8: {
    std::shared_ptr<std::vector<int8_t>> d =
        std::make_shared<std::vector<int8_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_UINT8: {
    std::shared_ptr<std::vector<uint8_t>> d =
        std::make_shared<std::vector<uint8_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_INT16: {
    std::shared_ptr<std::vector<int16_t>> d =
        std::make_shared<std::vector<int16_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_UINT16: {
    std::shared_ptr<std::vector<uint16_t>> d =
        std::make_shared<std::vector<uint16_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_INT32: {
    std::shared_ptr<std::vector<int32_t>> d =
        std::make_shared<std::vector<int32_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_UINT32: {
    std::shared_ptr<std::vector<uint32_t>> d =
        std::make_shared<std::vector<uint32_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_INT64:
  case TILEDB_DATETIME_YEAR:
  case TILEDB_DATETIME_MONTH:
  case TILEDB_DATETIME_WEEK:
  case TILEDB_DATETIME_DAY:
  case TILEDB_DATETIME_HR:
  case TILEDB_DATETIME_MIN:
  case TILEDB_DATETIME_SEC:
  case TILEDB_DATETIME_MS:
  case TILEDB_DATETIME_US:
  case TILEDB_DATETIME_NS:
  case TILEDB_DATETIME_PS:
  case TILEDB_DATETIME_FS:
  case TILEDB_DATETIME_AS:
  case TILEDB_TIME_HR:
  case TILEDB_TIME_MIN:
  case TILEDB_TIME_SEC:
  case TILEDB_TIME_MS:
  case TILEDB_TIME_US:
  case TILEDB_TIME_NS:
  case TILEDB_TIME_PS:
  case TILEDB_TIME_FS:
  case TILEDB_TIME_AS: {
    std::shared_ptr<std::vector<int64_t>> d =
        std::make_shared<std::vector<int64_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_UINT64: {
    std::shared_ptr<std::vector<uint64_t>> d =
        std::make_shared<std::vector<uint64_t>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_FLOAT32: {
    std::shared_ptr<std::vector<float>> d =
        std::make_shared<std::vector<float>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  case TILEDB_FLOAT64: {
    std::shared_ptr<std::vector<double>> d =
        std::make_shared<std::vector<double>>();
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    d->push_back(1);
    query.set_subarray(*d);
    buffers.emplace_back(nullptr, std::move(d), nullptr);
    break;
  }
  default: {
    assert(false);
  }
  }

  // Set the buffer for each attribute
  for (const auto &attribute : array->schema().attributes()) {
    auto buffer = addDataToQuery(
        &query, attribute.first, attribute.second.type(),
        attribute.second.variable_sized(), attribute.second.nullable());
    buffers.push_back(std::move(buffer));
  }
  Query::Status status = query.submit();

  delete array;
  return status;
}

/**
 * Helper function for setting generic value datatypes that may be
 * var-sized and/or nullable.
 * @param query
 * @param attributeName
 * @param variableLength
 * @param nullable
 * @param offsets
 * @param values
 * @param validity
 * @return std::tuple
 */
template <typename T>
std::tuple<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>,
           std::unique_ptr<std::vector<uint8_t>>>
set_buffer_wrapper(Query *query, const std::string &attributeName,
                   const bool variableLength, const bool nullable,
                   std::unique_ptr<std::vector<uint64_t>> &&offsets,
                   std::shared_ptr<std::vector<T>> &&values,
                   std::unique_ptr<std::vector<uint8_t>> &&validity) {

  if (variableLength) {
    if (!nullable) {
      query->set_buffer(attributeName, *offsets, *values);
    } else {
      query->set_buffer_nullable(attributeName, *offsets, *values, *validity);
    }
  } else {
    if (!nullable) {
      query->set_buffer(attributeName, *values);
    } else {
      query->set_buffer_nullable(attributeName, *values, *validity);
    }
  }

  return std::make_tuple(std::move(offsets), std::move(values),
                         std::move(validity));
}

/**
 * Helper function to add data to a query
 * @param query
 * @param attributeName
 * @param datatype
 * @param variableLength
 * @return
 */
std::tuple<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>,
           std::unique_ptr<std::vector<uint8_t>>>
addDataToQuery(Query *query, std::string attributeName,
               tiledb_datatype_t datatype, bool variableLength, bool nullable) {
  std::unique_ptr<std::vector<uint64_t>> offsets =
      std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>);
  offsets->push_back(0);

  std::unique_ptr<std::vector<uint8_t>> validity =
      std::unique_ptr<std::vector<uint8_t>>(new std::vector<uint8_t>);
  validity->push_back(1);

  switch (datatype) {
  case TILEDB_INT8: {
    std::shared_ptr<std::vector<int8_t>> values =
        std::make_shared<std::vector<int8_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_BLOB: {
    std::shared_ptr<std::vector<std::byte>> values =
        std::make_shared<std::vector<std::byte>>();
    values->push_back(std::byte(1));
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_UINT8: {
    std::shared_ptr<std::vector<uint8_t>> values =
        std::make_shared<std::vector<uint8_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_INT16: {
    std::shared_ptr<std::vector<int16_t>> values =
        std::make_shared<std::vector<int16_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_UINT16: {
    std::shared_ptr<std::vector<uint16_t>> values =
        std::make_shared<std::vector<uint16_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_INT32: {
    std::shared_ptr<std::vector<int32_t>> values =
        std::make_shared<std::vector<int32_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_UINT32: {
    std::shared_ptr<std::vector<uint32_t>> values =
        std::make_shared<std::vector<uint32_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_INT64:
  case TILEDB_DATETIME_YEAR:
  case TILEDB_DATETIME_MONTH:
  case TILEDB_DATETIME_WEEK:
  case TILEDB_DATETIME_DAY:
  case TILEDB_DATETIME_HR:
  case TILEDB_DATETIME_MIN:
  case TILEDB_DATETIME_SEC:
  case TILEDB_DATETIME_MS:
  case TILEDB_DATETIME_US:
  case TILEDB_DATETIME_NS:
  case TILEDB_DATETIME_PS:
  case TILEDB_DATETIME_FS:
  case TILEDB_DATETIME_AS:
  case TILEDB_TIME_HR:
  case TILEDB_TIME_MIN:
  case TILEDB_TIME_SEC:
  case TILEDB_TIME_MS:
  case TILEDB_TIME_US:
  case TILEDB_TIME_NS:
  case TILEDB_TIME_PS:
  case TILEDB_TIME_FS:
  case TILEDB_TIME_AS: {
    std::shared_ptr<std::vector<int64_t>> values =
        std::make_shared<std::vector<int64_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_UINT64: {
    std::shared_ptr<std::vector<uint64_t>> values =
        std::make_shared<std::vector<uint64_t>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_FLOAT32: {
    std::shared_ptr<std::vector<float>> values =
        std::make_shared<std::vector<float>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_FLOAT64: {
    std::shared_ptr<std::vector<double>> values =
        std::make_shared<std::vector<double>>();
    values->push_back(1);
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_CHAR:
  case TILEDB_STRING_ASCII:
  case TILEDB_STRING_UTF8:
  case TILEDB_ANY: {
    std::shared_ptr<std::vector<char>> values =
        std::make_shared<std::vector<char>>();
    values->push_back('1');
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_STRING_UTF16:
  case TILEDB_STRING_UCS2: {
    std::shared_ptr<std::vector<char16_t>> values =
        std::make_shared<std::vector<char16_t>>();
    values->push_back(u'1');
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  case TILEDB_STRING_UTF32:
  case TILEDB_STRING_UCS4: {
    std::shared_ptr<std::vector<char32_t>> values =
        std::make_shared<std::vector<char32_t>>();
    values->push_back(U'1');
    return set_buffer_wrapper(query, attributeName, variableLength, nullable,
                              std::move(offsets), std::move(values),
                              std::move(validity));
  }
  }
  return std::make_tuple(nullptr, nullptr, nullptr);
}

bool build_homogeneous_arrays(const Context &ctx, const std::string &array_base,
                              const std::string version,
                              const tiledb_array_type_t array_type,
                              const tiledb_encryption_type_t encryption_type) {

  const std::vector<tiledb_datatype_t> dimension_datatypes =
      array_type == tiledb_array_type_t::TILEDB_DENSE
          ? dimension_dense_datatypes
          : dimension_sparse_datatypes;

  // Build an array for each dimension type
  for (auto datatype : dimension_datatypes) {
    std::stringstream array_name;
    array_name << array_base << "/" << ArraySchema::to_str(array_type) << "_";
    array_name << version << "_" + impl::type_to_str(datatype);
    array_name << ((encryption_type ==
                    tiledb_encryption_type_t::TILEDB_NO_ENCRYPTION)
                       ? ""
                       : "_encryption_AES_256_GCM");
    if (Object::object(ctx, array_name.str()).type() == Object::Type::Array) {
      continue;
    }

    std::cout << "Creating array " << array_name.str() << std::endl;
    createArray(ctx, array_name.str(), array_type, datatype, datatype,
                encryption_type);
    std::cout << "Created array " << array_name.str() << std::endl;
    if (array_type == tiledb_array_type_t::TILEDB_DENSE) {
      if (write_data_dense(ctx, array_name.str(), datatype, encryption_type) !=
          Query::Status::COMPLETE) {
        std::cerr << "Writing data for " << array_name.str() << " failed!"
                  << std::endl;
        return false;
      }
    } else {
      if (write_data_sparse(ctx, array_name.str(), datatype, datatype,
                            encryption_type) != Query::Status::COMPLETE) {
        std::cerr << "Writing data for " << array_name.str() << " failed!"
                  << std::endl;
        return false;
      }
    }
    std::cout << "Wrote array " << array_name.str() << std::endl;
  }

  return true;
}

bool build_heterogeneous_arrays(
    const Context &ctx, const std::string &array_base,
    const std::string version, const tiledb_array_type_t array_type,
    const tiledb_encryption_type_t encryption_type) {

  // Heterogeneous dimension types are only applicable to
  // sparse arrays.
  if (array_type == tiledb_array_type_t::TILEDB_DENSE) {
    return true;
  }

  for (size_t i = 0; i < heterogeneous_dimension_datatypes.size() - 1; ++i) {
    for (size_t j = i + 1; j < heterogeneous_dimension_datatypes.size(); ++j) {
      const tiledb_datatype_t dim1_type = heterogeneous_dimension_datatypes[i];
      const tiledb_datatype_t dim2_type = heterogeneous_dimension_datatypes[j];

      std::stringstream array_name;
      array_name << array_base << "/" << ArraySchema::to_str(array_type) << "_";
      array_name << version;
      array_name << "_" << impl::type_to_str(dim1_type);
      array_name << "_" << impl::type_to_str(dim2_type);
      array_name << ((encryption_type ==
                      tiledb_encryption_type_t::TILEDB_NO_ENCRYPTION)
                         ? ""
                         : "_encryption_AES_256_GCM");

      if (Object::object(ctx, array_name.str()).type() == Object::Type::Array) {
        continue;
      }

      std::cout << "Creating array " << array_name.str() << std::endl;
      createArray(ctx, array_name.str(), array_type, dim1_type, dim2_type,
                  encryption_type);
      std::cout << "Created array " << array_name.str() << std::endl;
      if (write_data_sparse(ctx, array_name.str(), dim1_type, dim2_type,
                            encryption_type) != Query::Status::COMPLETE) {
        std::cerr << "Writing data for " << array_name.str() << " failed!"
                  << std::endl;
        return false;
      }
      std::cout << "Wrote array " << array_name.str() << std::endl;
    }
  }

  return true;
}

int main() {
  const std::tuple<int, int, int> &tiledbVersion = version();
  const std::string version = "v" + std::to_string(std::get<0>(tiledbVersion)) +
                              "_" + std::to_string(std::get<1>(tiledbVersion)) +
                              "_" + std::to_string(std::get<2>(tiledbVersion));
  std::string array_base = "arrays";

  // Create a TileDB context.
  Context ctx;
  // Create a group based on the tiledb version
  if (Object::object(ctx, array_base).type() != Object::Type::Group) {
    create_group(ctx, array_base);
  }
  // Create a group based on the tiledb version
  array_base += "/" + version;
  if (Object::object(ctx, array_base).type() != Object::Type::Group) {
    create_group(ctx, array_base);
  }

  // Build an array for each array type
  for (auto array_type : array_types) {
    for (auto encryption_type : encryption_types) {
      if (!build_homogeneous_arrays(ctx, array_base, version, array_type,
                                    encryption_type))
        return 1;
      if (!build_heterogeneous_arrays(ctx, array_base, version, array_type,
                                      encryption_type))
        return 1;
    }
  }

  return 0;
}
