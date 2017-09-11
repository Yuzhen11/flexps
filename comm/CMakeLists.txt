include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB comm-src-files
  mailbox.cpp)

add_library(comm-objs OBJECT ${comm-src-files})
set_property(TARGET comm-objs PROPERTY CXX_STANDARD 11)
add_dependencies(comm-objs ${external_project_dependencies})

# MailboxTest
add_executable(MailboxTest mailbox_test.cpp)
target_link_libraries(MailboxTest flexps)
target_link_libraries(MailboxTest ${HUSKY_EXTERNAL_LIB})
set_property(TARGET MailboxTest PROPERTY CXX_STANDARD 11)
add_dependencies(MailboxTest ${external_project_dependencies})