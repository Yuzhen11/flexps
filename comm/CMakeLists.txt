include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB comm-src-files
  mailbox.cpp
  sender.cpp
  local_channel.cpp
  channel.cpp)

add_library(comm-objs OBJECT ${comm-src-files})
set_property(TARGET comm-objs PROPERTY CXX_STANDARD 11)
add_dependencies(comm-objs ${external_project_dependencies})
