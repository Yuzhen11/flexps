include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# BasicExample
add_executable(BasicExample basic_example.cpp)
target_link_libraries(BasicExample flexps)
target_link_libraries(BasicExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET BasicExample PROPERTY CXX_STANDARD 11)
add_dependencies(BasicExample ${external_project_dependencies})

# SparseSSPExample
add_executable(SparseSSPExample sparse_ssp_example.cpp)
target_link_libraries(SparseSSPExample flexps)
target_link_libraries(SparseSSPExample profiler)  # compile with profiler
target_link_libraries(SparseSSPExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SparseSSPExample PROPERTY CXX_STANDARD 11)
add_dependencies(SparseSSPExample ${external_project_dependencies})

# KVStorePerformanceExample
add_executable(KVStorePerformanceExample kvstore_performance_example.cpp)
target_link_libraries(KVStorePerformanceExample flexps)
target_link_libraries(KVStorePerformanceExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET KVStorePerformanceExample PROPERTY CXX_STANDARD 11)
add_dependencies(KVStorePerformanceExample ${external_project_dependencies})
