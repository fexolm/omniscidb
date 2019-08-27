include_directories("$ENV{VTUNE_AMPLIFIER_2019_DIR}/include")
link_directories("$ENV{VTUNE_AMPLIFIER_2019_DIR}/lib64")
add_library(Fragmenter InsertOrderFragmenter.cpp SortedOrderFragmenter.cpp UpdelStorage.cpp TargetValueConvertersFactories.cpp InsertDataLoader.cpp)

target_link_libraries(Fragmenter ${Boost_THREAD_LIBRARY} libittnotify)
