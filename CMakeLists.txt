cmake_minimum_required(VERSION 3.10)
project(SkipList)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../bin)

# 指定头文件搜索路径
include_directories(include)

# 编译可执行文件（不需要 add_library）
add_executable(skiplist_test
    tests/SkipListTest.cpp
)
