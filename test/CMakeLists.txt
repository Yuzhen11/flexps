include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

add_library(flexps STATIC $<TARGET_OBJECTS:tmp-objs> 
  $<TARGET_OBJECTS:server-objs> 
  $<TARGET_OBJECTS:worker-objs> 
  $<TARGET_OBJECTS:comm-objs> 
  $<TARGET_OBJECTS:base-objs>
  $<TARGET_OBJECTS:driver-objs>
  )

# Unit Test
file(GLOB_RECURSE UNITTEST_FILES ${PROJECT_SOURCE_DIR}/*_test.cpp) 
include_directories(${GTEST_INCLUDE})
add_executable(HuskyUnitTest ${UNITTEST_FILES} test_main.cpp)
add_dependencies(HuskyUnitTest gtest)
target_link_libraries(HuskyUnitTest flexps)
target_link_libraries(HuskyUnitTest ${HUSKY_EXTERNAL_LIB})
target_link_libraries(HuskyUnitTest ${GTEST_LIBRARIES})
target_link_libraries(HuskyUnitTest ${GMOCK_LIBRARIES})
set_property(TARGET HuskyUnitTest PROPERTY CXX_STANDARD 11)
add_dependencies(HuskyUnitTest ${external_project_dependencies})

# TestWorker
add_executable(TestWorker test_worker.cpp)
target_link_libraries(TestWorker flexps)
target_link_libraries(TestWorker ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TestWorker PROPERTY CXX_STANDARD 11)
add_dependencies(TestWorker ${external_project_dependencies})
