
include (GNUInstallDirs)

if(GFLAGS_SEARCH_PATH)
    # Note: if using GFLAGS_SEARCH_PATH, the customized format is not activated.
    find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h PATHS ${GFLAGS_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    find_library(GFLAGS_LIBRARY NAMES gflags PATHS ${GFLAGS_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    message(STATUS "Found GFlags in search path ${GFLAGS_SEARCH_PATH}")
    message(STATUS "  (Headers)       ${GFLAGS_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GFLAGS_LIBRARY}")
else(GFLAGS_SEARCH_PATH)
    include(ExternalProject)
    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    ExternalProject_Add(
        gflags
        GIT_REPOSITORY "https://github.com/gflags/gflags"
        GIT_TAG v2.2.1
        PREFIX ${THIRDPARTY_DIR}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
        CMAKE_ARGS -DWITH_GFLAGS=OFF
        CMAKE_ARGS -DBUILD_TESTING=OFF
        CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF
        UPDATE_COMMAND ""
    )
    list(APPEND external_project_dependencies gflags)
    set(GFLAGS_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
    if(WIN32)
        set(GFLAGS_LIBRARY "${PROJECT_BINARY_DIR}/lib/libgflags.lib")
    else(WIN32)
        set(GFLAGS_LIBRARY "${PROJECT_BINARY_DIR}/lib/libgflags.a")
    endif(WIN32)
    message(STATUS "GFlags will be built as a third party")
    message(STATUS "  (Headers should be)       ${GFLAGS_INCLUDE_DIR}")
    message(STATUS "  (Library should be)       ${GFLAGS_LIBRARY}")
endif(GFLAGS_SEARCH_PATH)
