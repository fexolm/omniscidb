add_library(Planner Planner.cpp Planner.h)
target_compile_options(Planner PRIVATE -fPIC)
target_link_libraries(Planner Parser Analyzer)
