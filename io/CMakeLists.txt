include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB io-src-files
  coordinator.cpp
  hdfs_assigner.cpp
  master.cpp
  )

add_library(io-objs OBJECT ${io-src-files})
set_property(TARGET comm-objs PROPERTY CXX_STANDARD 11)
add_dependencies(io-objs ${external_project_dependencies})
