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
    TILEDB_INT8,
    TILEDB_UINT8,
    TILEDB_INT16,
    TILEDB_UINT16,
    TILEDB_INT32,
    TILEDB_UINT32,
    TILEDB_INT64,
    TILEDB_UINT64,
};

// Dimensions valid for sparse arrays
std::vector<tiledb_datatype_t> dimension_sparse_datatypes = {
    TILEDB_INT8,
    TILEDB_UINT8,
    TILEDB_INT16,
    TILEDB_UINT16,
    TILEDB_INT32,
    TILEDB_UINT32,
    TILEDB_INT64,
    TILEDB_UINT64,
    TILEDB_FLOAT32,
    TILEDB_FLOAT64,
};

// Attribute types to check, currently ignoring ANY and STRING_* due to c++ api limitations
std::vector<tiledb_datatype_t> attribute_types = {
    TILEDB_INT8,
    TILEDB_UINT8,
    TILEDB_INT16,
    TILEDB_UINT16,
    TILEDB_INT32,
    TILEDB_UINT32,
    TILEDB_INT64,
    TILEDB_UINT64,
    TILEDB_FLOAT32,
    TILEDB_FLOAT64,
    TILEDB_CHAR,
    /*TILEDB_STRING_ASCII,
    TILEDB_STRING_UTF8,
    TILEDB_STRING_UTF16,
    TILEDB_STRING_UTF32,
    TILEDB_STRING_UCS2,
    TILEDB_STRING_UCS4,
    TILEDB_ANY*/
};

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

std::vector<tiledb_encryption_type_t> encryption_types = {
    TILEDB_NO_ENCRYPTION,
    TILEDB_AES_256_GCM
};

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

std::pair<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>> addDataToQuery(Query *query, std::string attributeName, tiledb_datatype_t datatype, bool variableLength);

/**
 * Create an array on disk for a given array type and dimension type. All attribute types and filters will be used
 * @param ctx
 * @param array_name
 * @param array_type
 * @param dimension_type
 */
void createArray(Context ctx, std::string array_name, tiledb_array_type_t array_type, tiledb_datatype_t dimension_type, tiledb_encryption_type_t encryption_type) {
  Domain domain(ctx);

  // Build two dimensions rows and columns with domains [1,4]
  switch (dimension_type) {
    case TILEDB_INT8: {
      domain.add_dimension(Dimension::create<int8_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<int8_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_UINT8: {
      domain.add_dimension(Dimension::create<uint8_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<uint8_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_INT16: {
      domain.add_dimension(Dimension::create<int16_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<int16_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_UINT16: {
      domain.add_dimension(Dimension::create<uint16_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<uint16_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_INT32: {
      domain.add_dimension(Dimension::create<int32_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<int32_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_UINT32: {
      domain.add_dimension(Dimension::create<uint32_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<uint32_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_INT64: {
      domain.add_dimension(Dimension::create<int64_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<int64_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_UINT64: {
      domain.add_dimension(Dimension::create<uint64_t>(ctx, "rows", {{1, 4}}, 4))
          .add_dimension(Dimension::create<uint64_t>(ctx, "cols", {{1, 4}}, 4));
      break;
    }
    case TILEDB_FLOAT32: {
      domain.add_dimension(Dimension::create<float>(ctx, "rows", {{1, 4}}))
          .add_dimension(Dimension::create<float>(ctx, "cols", {{1, 4}}));
      break;
    }
    case TILEDB_FLOAT64: {
      domain.add_dimension(Dimension::create<double>(ctx, "rows", {{1, 4}}))
          .add_dimension(Dimension::create<double>(ctx, "cols", {{1, 4}}));
      break;
    }
    default: {
      assert(false);
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
        std::string attributeName =
            "attribute_" + impl::type_to_str(datatype) + "_" + Filter::to_str(filter);
        schema.add_attribute(Attribute(ctx, attributeName, datatype).set_filter_list(filterList));
      }

      // Create a variable length version of a given array type
      std::string varAttributeName =
          "attribute_variable_" + impl::type_to_str(datatype) + "_" + Filter::to_str(filter);
      Attribute varAttr = Attribute(ctx, varAttributeName, datatype);
      varAttr.set_filter_list(filterList);
      if (datatype != TILEDB_ANY) {
        varAttr.set_cell_val_num(TILEDB_VAR_NUM);
      }
      schema.add_attribute(varAttr);
    }
  }

  // Create the (empty) array on disk.
  if (encryption_type == TILEDB_NO_ENCRYPTION)
    Array::create(array_name, schema);
  else
    Array::create(array_name, schema, encryption_type, encryption_key.c_str(), encryption_key.size() * sizeof(char));
}

/**
 * Write a single data value of 1 to the the array for all attributes and dimension
 * @param ctx
 * @param array_name
 * @param dimensionType
 * @return
 */
Query::Status writeData(Context ctx, std::string array_name, tiledb_datatype_t dimensionType, tiledb_encryption_type_t encryption_type) {

  Array *array;
  if (encryption_type == TILEDB_NO_ENCRYPTION)
    array = new Array(ctx, array_name, TILEDB_WRITE);
  else
    array = new Array(ctx, array_name, TILEDB_WRITE, encryption_type, encryption_key.c_str(), encryption_key.size() * sizeof(char));
  Query query(ctx, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);


  // Hold a reference to the buffers so they exists until we are finished writting
  std::vector<std::pair<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>>> buffers;

  // Set the coordinates for the unordered write
  switch (dimensionType) {
    case TILEDB_INT8: {
      std::shared_ptr<std::vector<int8_t>> values = std::make_shared<std::vector<int8_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_UINT8: {
      std::shared_ptr<std::vector<uint8_t>> values = std::make_shared<std::vector<uint8_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_INT16: {
      std::shared_ptr<std::vector<int16_t>> values = std::make_shared<std::vector<int16_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_UINT16: {
      std::shared_ptr<std::vector<uint16_t>> values = std::make_shared<std::vector<uint16_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_INT32: {
      std::shared_ptr<std::vector<int32_t>> values = std::make_shared<std::vector<int32_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_UINT32: {
      std::shared_ptr<std::vector<uint32_t>> values = std::make_shared<std::vector<uint32_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_INT64: {
      std::shared_ptr<std::vector<int64_t>> values = std::make_shared<std::vector<int64_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_UINT64: {
      std::shared_ptr<std::vector<uint64_t>> values = std::make_shared<std::vector<uint64_t>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_FLOAT32: {
      std::shared_ptr<std::vector<float>> values = std::make_shared<std::vector<float>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    case TILEDB_FLOAT64: {
      std::shared_ptr<std::vector<double>> values = std::make_shared<std::vector<double>>();
      values->push_back(1);
      values->push_back(1);
      query.set_coordinates(*values);
      buffers.emplace_back(nullptr, std::move(values));
      break;
    }
    default: {
      assert(false);
    }
  }

  // Set the buffer for each attribute
  for(auto attribute : array->schema().attributes()) {
        auto buffer = addDataToQuery(&query, attribute.first, attribute.second.type(), attribute.second.variable_sized());
        buffers.push_back(std::move(buffer));
  }
  Query::Status status = query.submit();

  // Finalize the query since we are doing unordered writes
  query.finalize();
  delete array;
  return status;
}

/**
 * Helper function to add data to a query
 * @param query
 * @param attributeName
 * @param datatype
 * @param variableLength
 * @return
 */
std::pair<std::unique_ptr<std::vector<uint64_t>>, std::shared_ptr<void>> addDataToQuery(Query *query, std::string attributeName, tiledb_datatype_t datatype, bool variableLength) {
  std::unique_ptr<std::vector<uint64_t>> offsets = std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>);
  offsets->push_back(0);
  switch (datatype) {
    case TILEDB_INT8: {
      std::shared_ptr<std::vector<int8_t>> values = std::make_shared<std::vector<int8_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_UINT8: {
      std::shared_ptr<std::vector<uint8_t>> values = std::make_shared<std::vector<uint8_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_INT16: {
      std::shared_ptr<std::vector<int16_t>> values = std::make_shared<std::vector<int16_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_UINT16: {
      std::shared_ptr<std::vector<uint16_t>> values = std::make_shared<std::vector<uint16_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_INT32: {
      std::shared_ptr<std::vector<int32_t>> values = std::make_shared<std::vector<int32_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_UINT32: {
      std::shared_ptr<std::vector<uint32_t>> values = std::make_shared<std::vector<uint32_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_INT64: {
      std::shared_ptr<std::vector<int64_t>> values = std::make_shared<std::vector<int64_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_UINT64: {
      std::shared_ptr<std::vector<uint64_t>> values = std::make_shared<std::vector<uint64_t>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_FLOAT32: {
      std::shared_ptr<std::vector<float>> values = std::make_shared<std::vector<float>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_FLOAT64: {
      std::shared_ptr<std::vector<double>> values = std::make_shared<std::vector<double>>();
      values->push_back(1);
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
    case TILEDB_CHAR:
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UTF8:
    case TILEDB_STRING_UTF16:
    case TILEDB_STRING_UTF32:
    case TILEDB_STRING_UCS2:
    case TILEDB_STRING_UCS4:
    case TILEDB_ANY: {
      std::shared_ptr<std::vector<char>> values = std::make_shared<std::vector<char>>();
      values->push_back('1');
      if (variableLength) {
        query->set_buffer(attributeName, *offsets, *values);
      } else {
        query->set_buffer(attributeName, *values);
      }
      return std::make_pair(std::move(offsets), std::move(values));
    }
  }
  return std::make_pair(nullptr, nullptr);
}

int main() {
  const std::tuple<int, int, int> &tiledbVersion = version();
  std::string tiledbVersionStr = "v" + std::to_string(std::get<0>(tiledbVersion)) + "_" + std::to_string(std::get<1>(tiledbVersion)) + "_" + std::to_string(std::get<2>(tiledbVersion));

  std::string array_base = "arrays";

  // Create a TileDB context.
  Context ctx;
  // Create a group based on the tiledb version
  if (Object::object(ctx, array_base).type() != Object::Type::Group) {
    create_group(ctx, array_base);
  }
  // Create a group based on the tiledb version
  array_base += "/" + tiledbVersionStr;
  if (Object::object(ctx, array_base).type() != Object::Type::Group) {
    create_group(ctx, array_base);
  }

  // Build an array for each array type
  for (auto arraytype : array_types) {
    for (auto encryption_type : encryption_types) {
      std::vector<tiledb_datatype_t> dimension_datatypes = dimension_dense_datatypes;
      if (arraytype == tiledb_array_type_t::TILEDB_SPARSE)
        dimension_datatypes = dimension_sparse_datatypes;

      // Build an array for each dimension type
      for (auto datatype : dimension_datatypes) {
        std::stringstream array_name;
        array_name << array_base << "/" << ArraySchema::to_str(arraytype) << "_";
        array_name << tiledbVersionStr << "_" + impl::type_to_str(datatype);
        array_name << ((encryption_type == tiledb_encryption_type_t::TILEDB_NO_ENCRYPTION) ? "" : "_encryption_AES_256_GCM");
        if (Object::object(ctx, array_name.str()).type() == Object::Type::Array) {
          continue;
        }

        createArray(ctx, array_name.str(), arraytype, datatype, encryption_type);
        if (writeData(ctx, array_name.str(), datatype, encryption_type) != Query::Status::COMPLETE) {
          std::cerr << "Writing data for " << array_name.str() << " failed!" << std::endl;
          return 1;
        }
      }
    }
  }
}
