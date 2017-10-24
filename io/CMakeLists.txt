include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB io-src-files coordinator.cpp)

if(LIBHDFS3_FOUND)
    file(GLOB io-src-hdfs-files hdfs_assigner.cpp hdfs_manager.cpp file_splitter.cpp)
    list(APPEND io-src-files ${io-src-hdfs-files})
endif(LIBHDFS3_FOUND)

add_library(io-objs OBJECT ${io-src-files})
set_property(TARGET io-objs PROPERTY CXX_STANDARD 11)
add_dependencies(io-objs ${external_project_dependencies})
