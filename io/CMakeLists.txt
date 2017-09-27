include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB io-src-files
  coordinator.cpp
  hdfs_assigner.cpp
  master.cpp
  )

if(LIBHDFS3_FOUND)
    file(GLOB io-input-hdfs-src-files hdfs_file_splitter.cpp hdfs_binary_inputformat.cpp)
    list(APPEND io-input-src-files ${io-input-hdfs-src-files})
endif(LIBHDFS3_FOUND)

add_library(io-objs OBJECT ${io-src-files})
set_property(TARGET io-objs PROPERTY CXX_STANDARD 11)
add_dependencies(io-objs ${external_project_dependencies})
