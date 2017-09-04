
include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB tmp-src-files
  a.cpp)

add_library(tmp-objs OBJECT ${tmp-src-files})
set_property(TARGET tmp-objs PROPERTY CXX_STANDARD 11)
add_dependencies(tmp-objs ${external_project_dependencies})

