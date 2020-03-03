add_library(DBEngine SHARED DBEngine.cpp)
target_compile_options(DBEngine PRIVATE -fPIC)
target_link_libraries(DBEngine tinfo ${Arrow_LIBRARIES} ${LLVM_LINKER_FLAGS} QueryRunner DataMgr)

add_executable(DBEngineApp DBEngineApp.cpp)

target_link_libraries(DBEngineApp DBEngine)
