include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB task-src-files
  scheduler_thread.cpp
  worker_thread.cpp
  )

add_library(task-objs OBJECT ${task-src-files})
set_property(TARGET task-objs PROPERTY CXX_STANDARD 11)
add_dependencies(task-objs ${external_project_dependencies})
