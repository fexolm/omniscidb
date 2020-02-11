#include "DBEngine.h"
#include "../QueryRunner/QueryRunner.h"
#include "../DataMgr/ForeignStorage/ArrowCsvForeignStorage.h"
#include "../DataMgr/ForeignStorage/ForeignStorageInterface.h"


#define BASE_PATH "/localdisk/gal/wrap/omniscidb/build_cl/tmp"

using QR = QueryRunner::QueryRunner;

namespace Wrapper {

void run_ddl_statement(std::string input_str) {
  QR::get()->runDDLStatement(input_str);
}

void init() {
  QR::init(BASE_PATH);
  registerArrowCsvForeignStorage();
}

void destroy() {
  ForeignStorageInterface::destroy();
  QR::reset();
}

void run_simple_agg(std::string query_str) {
  auto rows = QR::get()->runSQL(query_str, ExecutorDeviceType::CPU, false);
}
}  // namespace Wrapper
