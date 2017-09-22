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
target_link_libraries(SparseSSPExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SparseSSPExample PROPERTY CXX_STANDARD 11)
add_dependencies(SparseSSPExample ${external_project_dependencies})

