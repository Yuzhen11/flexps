include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# GBDTExample
file(GLOB gbdt-src-files
  src/*.cpp
  )
add_executable(GBDTExample gbdt_example_hdfs.cpp ${gbdt-src-files})
target_link_libraries(GBDTExample flexps)
target_link_libraries(GBDTExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET GBDTExample PROPERTY CXX_STANDARD 11)
add_dependencies(GBDTExample ${external_project_dependencies})
