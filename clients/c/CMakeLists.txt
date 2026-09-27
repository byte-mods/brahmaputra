cmake_minimum_required(VERSION 3.10)
project(brahmaputra_c C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(BRP_WITH_ZLIB "Build gzip support with zlib" ON)
option(BRP_SANITIZE "Build with -fsanitize=address,undefined" OFF)

find_package(Threads REQUIRED)

add_library(brahmaputra STATIC
  src/protocol.c src/client.c src/producer.c src/consumer.c src/group.c)
target_include_directories(brahmaputra PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_options(brahmaputra PRIVATE -Wall -Wextra -Werror -pedantic)
target_link_libraries(brahmaputra PUBLIC Threads::Threads)

if(BRP_WITH_ZLIB)
  find_package(ZLIB REQUIRED)
  target_compile_definitions(brahmaputra PRIVATE BRP_WITH_ZLIB)
  target_link_libraries(brahmaputra PUBLIC ZLIB::ZLIB)
endif()

add_executable(manual_test test/manual_test.c)
target_compile_options(manual_test PRIVATE -Wall -Wextra -Werror -pedantic)
target_link_libraries(manual_test PRIVATE brahmaputra)

if(BRP_SANITIZE)
  foreach(t brahmaputra manual_test)
    target_compile_options(${t} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${t} PRIVATE -fsanitize=address,undefined)
  endforeach()
endif()
