include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB worker-src-files
  app_blocker.cpp
  worker_helper_thread.cpp
  model_init_thread.cpp)

add_library(worker-objs OBJECT ${worker-src-files})
set_property(TARGET worker-objs PROPERTY CXX_STANDARD 11)
add_dependencies(worker-objs ${external_project_dependencies})

