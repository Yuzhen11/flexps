include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# SGDExample
if(LIBHDFS3_FOUND)
  file(GLOB sgd-lib-files
    lib/optimizers.hpp
    lib/objectives.hpp
  )
  add_executable(SGDExample sgd_example.cpp ${sgd-lib-files})
  target_link_libraries(SGDExample flexps)
  target_link_libraries(SGDExample profiler)
  target_link_libraries(SGDExample ${HUSKY_EXTERNAL_LIB})
  set_property(TARGET SGDExample PROPERTY CXX_STANDARD 11)
  add_dependencies(SGDExample ${external_project_dependencies})
endif(LIBHDFS3_FOUND)
