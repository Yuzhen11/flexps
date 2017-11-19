include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# KMeansExample
if(LIBHDFS3_FOUND)
  file(GLOB kmeans-src-files
    kmeans_helper.cpp
  )
  add_executable(KMeansExample kmeans_example.cpp ${kmeans-src-files})
  target_link_libraries(KMeansExample flexps)
  target_link_libraries(KMeansExample profiler)
  target_link_libraries(KMeansExample ${HUSKY_EXTERNAL_LIB})
  set_property(TARGET KMeansExample PROPERTY CXX_STANDARD 11)
  add_dependencies(KMeansExample ${external_project_dependencies})
endif(LIBHDFS3_FOUND)
