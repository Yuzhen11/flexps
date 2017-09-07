# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


project(flexps)
cmake_minimum_required(VERSION 3.0.2)

# CMake Options
set(CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS_DEBUG "-g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -w")
    set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-Wno-deprecated-declarations")
endif()

find_package(Threads)

# External Dependencies
include(cmake/modules/glog.cmake)
include(cmake/modules/gtest.cmake)
include(cmake/modules/zeromq.cmake)

# External Includes
set(HUSKY_EXTERNAL_INCLUDE ${ZMQ_INCLUDE_DIR} ${GLOG_INCLUDE_DIR})

# External Libraries
set(HUSKY_EXTERNAL_LIB ${GLOG_LIBRARY} ${ZMQ_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})

add_subdirectory(tmp)
add_subdirectory(base)
add_subdirectory(server)
add_subdirectory(worker)

add_subdirectory(test)
