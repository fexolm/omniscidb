#include <string>

namespace Wrapper {

void run_ddl_statement(std::string input_str);

void init();

void destroy();

void run_simple_agg(std::string query_str);

}  // namespace Wrapper