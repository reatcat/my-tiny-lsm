# CMake 最低版本要求
cmake_minimum_required(VERSION 3.14)

# 项目名称
project(my_tiny_lsm)

# 设置C++标准为C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ----------------------------------------------------------------------------
# 自动下载并配置 Google Test
# ----------------------------------------------------------------------------
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# 对于Windows: 防止覆盖父项目的编译器/链接器设置
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
  spdlog
  URL https://github.com/gabime/spdlog/archive/refs/tags/v1.14.1.zip
)
FetchContent_MakeAvailable(spdlog)

# ----------------------------------------------------------------------------
# 定义你的项目库
# ----------------------------------------------------------------------------

# 将你的 skiplist 实现编译成一个静态库
# 这样做可以将你的库代码与测试代码清晰地分离开
add_library(
    skiplist_lib
    src/skiplist/skiplist.cpp
    src/iterator/iterator.cpp
)

# ${CMAKE_CURRENT_SOURCE_DIR} 指向项目根目录
target_include_directories(skiplist_lib PUBLIC 
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_link_libraries(skiplist_lib PUBLIC spdlog::spdlog)
# ----------------------------------------------------------------------------
# 定义测试可执行文件
# ----------------------------------------------------------------------------

# 启用测试功能
enable_testing()

# 创建测试可执行文件
add_executable(
    run_tests
    tests/skiplistTEST.cpp
)

# 将你的库和 Google Test 链接到测试程序
# gtest_main 包含了 gtest 库和一个预编译的 main() 函数
target_link_libraries(
    run_tests
    PRIVATE
    skiplist_lib
    gtest_main
)

# 使用 include(GoogleTest) 来自动发现测试用例
include(GoogleTest)
gtest_discover_tests(run_tests)