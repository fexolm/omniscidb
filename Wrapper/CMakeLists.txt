add_library(DBEngine SHARED DBEngine.cpp)
target_compile_options(DBEngine PRIVATE -fPIC)
target_link_libraries(DBEngine tinfo ${LLVM_LINKER_FLAGS} QueryRunner DataMgr)

#add_executable(main main.cpp)

#target_link_libraries(main DBEngine)
