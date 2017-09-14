include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB driver-src-files
  node_parser.cpp
  simple_id_mapper.cpp
  engine.cpp
  )

add_library(driver-objs OBJECT ${driver-src-files})
set_property(TARGET driver-objs PROPERTY CXX_STANDARD 11)
add_dependencies(driver-objs ${external_project_dependencies})

# TestGFlags
# add_executable(TestGFlags test_gflags.cpp)
# target_link_libraries(TestGFlags flexps)
# target_link_libraries(TestGFlags ${HUSKY_EXTERNAL_LIB})
# set_property(TARGET TestGFlags PROPERTY CXX_STANDARD 11)
# add_dependencies(TestGFlags ${external_project_dependencies})
