cmake_minimum_required(VERSION 3.16)
project(brahmaputra_cpp VERSION 0.1.0 LANGUAGES CXX)

option(BRAHMAPUTRA_WITH_GZIP "Build the gzip codec in (requires zlib)" ON)
option(BRAHMAPUTRA_BUILD_TESTS "Build the manual_test end-to-end suite" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Threads REQUIRED)

add_library(brahmaputra
  src/protocol.cpp
  src/codec.cpp
  src/connection.cpp
  src/client.cpp
  src/group.cpp)
add_library(brahmaputra::brahmaputra ALIAS brahmaputra)
target_include_directories(brahmaputra PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_link_libraries(brahmaputra PUBLIC Threads::Threads)
target_compile_options(brahmaputra PRIVATE -Wall -Wextra -Wpedantic)

if(BRAHMAPUTRA_WITH_GZIP)
  find_package(ZLIB REQUIRED)
  target_link_libraries(brahmaputra PRIVATE ZLIB::ZLIB)
  target_compile_definitions(brahmaputra PRIVATE BRAHMAPUTRA_HAVE_ZLIB=1)
endif()

if(BRAHMAPUTRA_BUILD_TESTS)
  add_executable(manual_test test/manual_test.cpp)
  target_link_libraries(manual_test PRIVATE brahmaputra)
  target_compile_options(manual_test PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS brahmaputra ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)
install(DIRECTORY include/brahmaputra DESTINATION include)
