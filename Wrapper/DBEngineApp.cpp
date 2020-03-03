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

//#include <thrift/Thrift.h>
#include <array>
#include <boost/filesystem.hpp>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include "DBEngine.h"

//#include "Catalog/Catalog.h"
//#include "Import/Importer.h"
//#include "Shared/Logger.h"
//#include "Shared/mapdpath.h"
#include "boost/program_options.hpp"
//#include "QueryEngine/Descriptors/RowSetMemoryOwner.h"
//#include "QueryEngine/ArrowResultSet.h"
//#define CALCITEPORT 3279
//
//static const std::array<std::string, 3> SampleGeoFileNames{"us-states.json",
//                                                           "us-counties.json",
//                                                           "countries.json"};
//static const std::array<std::string, 3> SampleGeoTableNames{"omnisci_states",
//                                                            "omnisci_counties",
//                                                            "omnisci_countries"};
//bool g_enable_thrift_logs{false};
//
//int TestArrowResultSet() {
//    std::vector<TargetInfo> target_infos;
//    QueryMemoryDescriptor query_mem_desc;
//    std::shared_ptr<ResultSet> result_set(
//	new ResultSet(
//	target_infos,
//	ExecutorDeviceType::CPU,
//	query_mem_desc,
//	std::make_shared<RowSetMemoryOwner>(),
//	nullptr
//	)
//    );
//    const ResultSetStorage* result_set_storage = result_set->allocateStorage();
//    ArrowResultSet arr_result_set(result_set);
//    return 0;
//}
//
//#include <cstdint>
//#include <iostream>
//#include <vector>
//
//#include <arrow/api.h>
//
//using arrow::DoubleBuilder;
//using arrow::Int64Builder;
//using arrow::ListBuilder;
//
//// While we want to use columnar data structures to build efficient operations, we
//// often receive data in a row-wise fashion from other systems. In the following,
//// we want give a brief introduction into the classes provided by Apache Arrow by
//// showing how to transform row-wise data into a columnar table.
////
//// The data in this example is stored in the following struct:
//struct data_row {
//  int64_t id;
//  double cost;
//  std::vector<double> cost_components;
//};
//
//// Transforming a vector of structs into a columnar Table.
////
//// The final representation should be an `arrow::Table` which in turn
//// is made up of an `arrow::Schema` and a list of
//// `arrow::ChunkedArray` instances. As the first step, we will iterate
//// over the data and build up the arrays incrementally.  For this
//// task, we provide `arrow::ArrayBuilder` classes that help in the
//// construction of the final `arrow::Array` instances.
//
////
//// For each type, Arrow has a specially typed builder class. For the primitive
//// values `id` and `cost` we can use the respective `arrow::Int64Builder` and
//// `arrow::DoubleBuilder`. For the `cost_components` vector, we need to have two
//// builders, a top-level `arrow::ListBuilder` that builds the array of offsets and
//// a nested `arrow::DoubleBuilder` that constructs the underlying values array that
//// is referenced by the offsets in the former array.
//arrow::Status VectorToColumnarTable(const std::vector<struct data_row>& rows,
//                                    std::shared_ptr<arrow::Table>* table) {
//  // The builders are more efficient using
//  // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size of
//  // the underlying memory regions in-place. At the moment, arrow::jemalloc is only
//  // supported on Unix systems, not Windows.
//  arrow::MemoryPool* pool = arrow::default_memory_pool();
//
//  Int64Builder id_builder(pool);
//  DoubleBuilder cost_builder(pool);
//  ListBuilder components_builder(pool, std::make_shared<DoubleBuilder>(pool));
//  // The following builder is owned by components_builder.
//  DoubleBuilder& cost_components_builder =
//      *(static_cast<DoubleBuilder*>(components_builder.value_builder()));
//
//  // Now we can loop over our existing data and insert it into the builders. The
//  // `Append` calls here may fail (e.g. we cannot allocate enough additional memory).
//  // Thus we need to check their return values. For more information on these values,
//  // check the documentation about `arrow::Status`.
//  for (const data_row& row : rows) {
//    ARROW_RETURN_NOT_OK(id_builder.Append(row.id));
//    ARROW_RETURN_NOT_OK(cost_builder.Append(row.cost));
//
//    // Indicate the start of a new list row. This will memorise the current
//    // offset in the values builder.
//    ARROW_RETURN_NOT_OK(components_builder.Append());
//    // Store the actual values. The final nullptr argument tells the underyling
//    // builder that all added values are valid, i.e. non-null.
//    ARROW_RETURN_NOT_OK(cost_components_builder.AppendValues(row.cost_components.data(),
//                                                             row.cost_components.size()));
//  }
//
//  // At the end, we finalise the arrays, declare the (type) schema and combine them
//  // into a single `arrow::Table`:
//  std::shared_ptr<arrow::Array> id_array;
//  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
//  std::shared_ptr<arrow::Array> cost_array;
//  ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));
//  // No need to invoke cost_components_builder.Finish because it is implied by
//  // the parent builder's Finish invocation.
//  std::shared_ptr<arrow::Array> cost_components_array;
//  ARROW_RETURN_NOT_OK(components_builder.Finish(&cost_components_array));
//
//  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
//      arrow::field("id", arrow::int64()), arrow::field("cost", arrow::float64()),
//      arrow::field("cost_components", arrow::list(arrow::float64()))};
//
//  auto schema = std::make_shared<arrow::Schema>(schema_vector);
//
//  // The final `table` variable is the one we then can pass on to other functions
//  // that can consume Apache Arrow memory structures. This object has ownership of
//  // all referenced data, thus we don't have to care about undefined references once
//  // we leave the scope of the function building the table and its underlying arrays.
//  *table = arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
//
//  return arrow::Status::OK();
//}
//
//arrow::Status ColumnarTableToVector(const std::shared_ptr<arrow::Table>& table,
//                                    std::vector<struct data_row>* rows) {
//  // To convert an Arrow table back into the same row-wise representation as in the
//  // above section, we first will check that the table conforms to our expected
//  // schema and then will build up the vector of rows incrementally.
//  //
//  // For the check if the table is as expected, we can utilise solely its schema.
//  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
//      arrow::field("id", arrow::int64()), arrow::field("cost", arrow::float64()),
//      arrow::field("cost_components", arrow::list(arrow::float64()))};
//  auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);
//
//  if (!expected_schema->Equals(*table->schema())) {
//    // The table doesn't have the expected schema thus we cannot directly
//    // convert it to our target representation.
//    return arrow::Status::Invalid("Schemas are not matching!");
//  }
//
//  // As we have ensured that the table has the expected structure, we can unpack the
//  // underlying arrays. For the primitive columns `id` and `cost` we can use the high
//  // level functions to get the values whereas for the nested column
//  // `cost_components` we need to access the C-pointer to the data to copy its
//  // contents into the resulting `std::vector<double>`. Here we need to be care to
//  // also add the offset to the pointer. This offset is needed to enable zero-copy
//  // slicing operations. While this could be adjusted automatically for double
//  // arrays, this cannot be done for the accompanying bitmap as often the slicing
//  // border would be inside a byte.
//
//  auto ids =
//      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->data()->chunk(0));
//  auto costs =
//      std::static_pointer_cast<arrow::DoubleArray>(table->column(1)->data()->chunk(0));
//  auto cost_components =
//      std::static_pointer_cast<arrow::ListArray>(table->column(2)->data()->chunk(0));
//  auto cost_components_values =
//      std::static_pointer_cast<arrow::DoubleArray>(cost_components->values());
//  // To enable zero-copy slices, the native values pointer might need to account
//  // for this slicing offset. This is not needed for the higher level functions
//  // like Value(_) that already account for this offset internally.
//  const double* ccv_ptr = cost_components_values->data()->GetValues<double>(1);
//
//  for (int64_t i = 0; i < table->num_rows(); i++) {
//    // Another simplification in this example is that we assume that there are
//    // no null entries, e.g. each row is fill with valid values.
//    int64_t id = ids->Value(i);
//    double cost = costs->Value(i);
//    const double* first = ccv_ptr + cost_components->value_offset(i);
//    const double* last = ccv_ptr + cost_components->value_offset(i + 1);
//    std::vector<double> components_vec(first, last);
//    rows->push_back({id, cost, components_vec});
//  }
//
//  return arrow::Status::OK();
//}
//
//#define EXIT_ON_FAILURE(expr)                      \
//  do {                                             \
//    arrow::Status status_ = (expr);                \
//    if (!status_.ok()) {                           \
//      std::cerr << status_.message() << std::endl; \
//      return EXIT_FAILURE;                         \
//    }                                              \
//  } while (0);
//
//int TestAroowSample() {
//  std::vector<data_row> rows = {
//      {1, 1.0, {1.0}}, {2, 2.0, {1.0, 2.0}}, {3, 3.0, {1.0, 2.0, 3.0}}};
//
//  std::shared_ptr<arrow::Table> table;
//  EXIT_ON_FAILURE(VectorToColumnarTable(rows, &table));
//
//  std::vector<data_row> expected_rows;
//  EXIT_ON_FAILURE(ColumnarTableToVector(table, &expected_rows));
//
//  assert(rows.size() == expected_rows.size());
//
//  return EXIT_SUCCESS;
//}
//
//int CreateDB(std::string& base_path, bool force, bool skip_geo) {
//  if (!boost::filesystem::exists(base_path)) {
//    std::cerr << "Catalog basepath " + base_path + " does not exist.\n";
//    return 1;
//  }
//  std::string catalogs_path = base_path + "/mapd_catalogs";
//  if (boost::filesystem::exists(catalogs_path)) {
//    if (force) {
//      boost::filesystem::remove_all(catalogs_path);
//    } else {
//      std::cerr << "OmniSci catalogs already initialized at " + base_path +
//                       ". Use -f to force reinitialization.\n";
//      return 1;
//    }
//  }
//  std::string data_path = base_path + "/mapd_data";
//  if (boost::filesystem::exists(data_path)) {
//    if (force) {
//      boost::filesystem::remove_all(data_path);
//    } else {
//      std::cerr << "OmniSci data directory already exists at " + base_path +
//                       ". Use -f to force reinitialization.\n";
//      return 1;
//    }
//  }
//  std::string export_path = base_path + "/mapd_export";
//  if (boost::filesystem::exists(export_path)) {
//    if (force) {
//      boost::filesystem::remove_all(export_path);
//    } else {
//      std::cerr << "OmniSci export directory already exists at " + base_path +
//                       ". Use -f to force reinitialization.\n";
//      return 1;
//    }
//  }
//  if (!boost::filesystem::create_directory(catalogs_path)) {
//    std::cerr << "Cannot create mapd_catalogs subdirectory under " << base_path
//              << std::endl;
//  }
//  if (!boost::filesystem::create_directory(export_path)) {
//    std::cerr << "Cannot create mapd_export subdirectory under " << base_path
//              << std::endl;
//  }
//
//  try {
//    MapDParameters mapd_parms;
//    auto dummy =
//        std::make_shared<Data_Namespace::DataMgr>(data_path, mapd_parms, false, 0);
//    auto calcite = std::make_shared<Calcite>(-1, CALCITEPORT, base_path, 1024);
//    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
//    sys_cat.init(base_path, dummy, {}, calcite, true, false, {});
//
//    if (!skip_geo) {
//      // Add geo samples to the system database using the root user
//      Catalog_Namespace::DBMetadata cur_db;
//      const std::string db_name(OMNISCI_DEFAULT_DB);
//      CHECK(sys_cat.getMetadataForDB(db_name, cur_db));
//      auto cat = Catalog_Namespace::Catalog::get(
//          base_path, cur_db, dummy, std::vector<LeafHostInfo>(), calcite, false);
//      Catalog_Namespace::UserMetadata user;
//      CHECK(sys_cat.getMetadataForUser(OMNISCI_ROOT_USER, user));
//
//      Importer_NS::ImportDriver import_driver(cat, user);
//
//      const size_t num_samples = SampleGeoFileNames.size();
//      for (size_t i = 0; i < num_samples; i++) {
//        const std::string table_name = SampleGeoTableNames[i];
//        const std::string file_name = SampleGeoFileNames[i];
//
//        const auto file_path = boost::filesystem::path(
//            mapd_root_abs_path() + "/ThirdParty/geo_samples/" + file_name);
//        if (!boost::filesystem::exists(file_path)) {
//          throw std::runtime_error(
//              "Unable to populate geo sample data. File does not exist: " +
//              file_path.string());
//        }
//
//        import_driver.importGeoTable(file_path.string(), table_name, true, true, false);
//      }
//    }
//
//  } catch (std::exception& e) {
//    std::cerr << "Exception: " << e.what() << "\n";
//  }
//  return 0;
//}


using namespace OmnisciDbEngine;

int main(int argc, char* argv[]) {
  std::string base_path;
  bool force = false;
  bool skip_geo = false;
  namespace po = boost::program_options;

  po::options_description desc("Options");
  desc.add_options()("help,h", "Print help messages ")(
      "data",
      po::value<std::string>(&base_path)->required(),
      "Directory path to OmniSci catalogs")(
      "force,f", "Force overwriting of existing OmniSci instance")(
      "skip-geo", "Skip inserting sample geo data");

//  logger::LogOptions log_options(argv[0]);
//  desc.add(log_options.get_options());

  po::positional_options_description positionalOptions;
  positionalOptions.add("data", 1);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv)
                  .options(desc)
                  .positional(positionalOptions)
                  .run(),
              vm);
    if (vm.count("help")) {
      std::cout << "Usage: initdb [-f] <catalog path>\n";
      return 0;
    }
    if (vm.count("force")) {
      force = true;
    }
    if (vm.count("skip-geo")) {
      skip_geo = true;
    }
    po::notify(vm);
	std::cout << "Create engine from " << base_path << std::endl;
//	std::unique_ptr<DBEngine> 
	auto eng = DBEngine::Create(base_path.c_str());
	std::cout << "Engine created" << std::endl;
//	eng->ExecuteDDL("CREATE TABLE Testt (fld1 BIGINT)");
//	std::cout << "CREATE TABLE - OK" << std::endl;
        auto aCursor = eng->ExecuteDML("SELECT count(id) FROM omnisci_states where abbr = 'CA'");
	std::cout << "SELECT - OK" << std::endl;
	std::cout << "Records: " << aCursor->GetRowCount() << std::endl;
        auto aBatch = aCursor->GetArrowRecordBatch();
	std::cout << "GET RECORD BATCH - OK" << std::endl;
	eng->Reset();
	std::cout << "Engine reseted" << std::endl;
  } catch (boost::program_options::error& e) {
    std::cerr << "Usage Error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
