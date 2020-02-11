add_library(Fragmenter InsertOrderFragmenter.cpp SortedOrderFragmenter.cpp UpdelStorage.cpp TargetValueConvertersFactories.cpp InsertDataLoader.cpp)
target_compile_options(Fragmenter PRIVATE -fPIC)
target_link_libraries(Fragmenter ${Boost_THREAD_LIBRARY})
