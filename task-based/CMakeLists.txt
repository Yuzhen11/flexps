include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB task-based-src-files
  scheduler_thread.cpp
  worker_thread.cpp
  )

add_library(task-based-objs OBJECT ${task-based-src-files})
set_property(TARGET task-based-objs PROPERTY CXX_STANDARD 11)
add_dependencies(task-based-objs ${external_project_dependencies})
