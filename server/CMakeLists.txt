include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB server-src-files
  ssp_model.cpp
  asp_model.cpp
  bsp_model.cpp
  progress_tracker.cpp
  server_thread.cpp
  pending_buffer.cpp
  sparsessp/sparse_pending_buffer.cpp
  sparsessp/sparse_conflict_detector.cpp
  sparsessp/sparse_ssp_model.cpp
  sparsessp/unordered_map_sparse_ssp_recorder.cpp
  sparsessp/vector_sparse_ssp_recorder.cpp
  )

add_library(server-objs OBJECT ${server-src-files})
set_property(TARGET server-objs PROPERTY CXX_STANDARD 11)
add_dependencies(server-objs ${external_project_dependencies})
