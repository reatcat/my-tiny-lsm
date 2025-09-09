#include "skiplist/skiplist.h" // 引入您自己的 skiplist 头文件
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip> // 用于 std::setw 和 std::setfill
#include <random>
#include <sstream> // 用于 std::ostringstream
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace my_tiny_lsm;

// 测试基本插入、查找和删除
TEST(MySkiplistTest, BasicOperations) {
  Skiplist skiplist;

  // 测试插入和查找
  skiplist.put("key1", "value1", 10);
  auto it1 = skiplist.get("key1", 15);
  ASSERT_TRUE(it1.is_valid());
  EXPECT_EQ(it1.get_value(), "value1");

  // 测试更新 (通过插入一个带有新事务ID或相同事务ID的新版本)
  skiplist.put("key1", "new_value", 20);
  auto it2 = skiplist.get("key1", 25);
  ASSERT_TRUE(it2.is_valid());
  EXPECT_EQ(it2.get_value(), "new_value");

  // 测试删除
  skiplist.remove("key1");
  auto it3 = skiplist.get("key1", 25);
  EXPECT_FALSE(it3.is_valid());
}

// 测试迭代器功能
TEST(MySkiplistTest, Iterator) {
  Skiplist skiplist;
  skiplist.put("key1", "value1", 10);
  skiplist.put("key3", "value3", 10);
  skiplist.put("key2", "value2", 10);

  // 测试迭代器遍历
  std::vector<std::pair<std::string, std::string>> result;
  for (auto it = skiplist.begin(); !it.is_end(); ++it) {
    result.emplace_back(it.get_key(), it.get_value());
  }

  // 验证结果数量和顺序
  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].first, "key1");
  EXPECT_EQ(result[1].first, "key2");
  EXPECT_EQ(result[2].first, "key3");
}

// 测试大量数据插入和查找
TEST(MySkiplistTest, LargeScaleInsertAndGet) {
  Skiplist skiplist;
  const int num_elements = 5000;

  // 插入大量数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    skiplist.put(key, value, 10);
  }

  // 验证插入的数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string expected_value = "value" + std::to_string(i);
    auto it = skiplist.get(key, 15);
    ASSERT_TRUE(it.is_valid());
    EXPECT_EQ(it.get_value(), expected_value);
  }
}

// 测试大量数据删除
TEST(MySkiplistTest, LargeScaleRemove) {
  Skiplist skiplist;
  const int num_elements = 5000;

  // 插入大量数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    skiplist.put(key, value, 10);
  }

  // 删除所有数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    skiplist.remove(key);
  }

  // 验证所有数据已被删除
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    EXPECT_FALSE(skiplist.get(key, 15).is_valid());
  }
  EXPECT_EQ(skiplist.get_size(), 0);
}


// 测试空跳表
TEST(MySkiplistTest, EmptySkipList) {
  Skiplist skiplist;

  // 验证空跳表的查找和删除
  EXPECT_FALSE(skiplist.get("nonexistent_key", 10).is_valid());
  skiplist.remove("nonexistent_key"); // 删除不存在的key不应导致崩溃
  EXPECT_EQ(skiplist.begin(), skiplist.end());
}


// 测试内存大小跟踪
TEST(MySkiplistTest, MemorySizeTracking) {
  Skiplist skiplist;

  // 插入数据
  std::string k1 = "key1", v1 = "value1";
  std::string k2 = "key2", v2 = "value22";
  skiplist.put(k1, v1, 10);
  skiplist.put(k2, v2, 10);

  // 验证内存大小
  size_t expected_size = k1.size() + v1.size() + sizeof(uint64_t) + 
                         k2.size() + v2.size() + sizeof(uint64_t);
  EXPECT_EQ(skiplist.get_size(), expected_size);

  // 删除数据
  skiplist.remove(k1);
  expected_size -= (k1.size() + v1.size() + sizeof(uint64_t));
  EXPECT_EQ(skiplist.get_size(), expected_size);

  // 清空跳表
  skiplist.clear();
  EXPECT_EQ(skiplist.get_size(), 0);
}

// 测试前缀迭代器
TEST(MySkiplistTest, IteratorPreffix) {
  Skiplist skiplist;
  skiplist.put("apple", "0", 10);
  skiplist.put("apple2", "1", 10);
  skiplist.put("apricot", "2", 10);
  skiplist.put("banana", "3", 10);
  skiplist.put("berry", "4", 10);
  skiplist.put("cherry", "5", 10);
  skiplist.put("cherry2", "6", 10);

  // 测试前缀 "ap"
  auto it_ap = skiplist.begin_preffix("ap");
  EXPECT_EQ(it_ap.get_key(), "apple");

  // 测试前缀 "b"
  auto it_b = skiplist.begin_preffix("b");
  EXPECT_EQ(it_b.get_key(), "banana");

  // 测试一个不存在的前缀
  auto it_z = skiplist.begin_preffix("z");
  EXPECT_TRUE(it_z.is_end());

  // 测试前缀结束位置
  auto end_it_a = skiplist.end_preffix("a");
  EXPECT_EQ(end_it_a.get_key(), "banana"); // 'a'系列的下一个是'b'

  auto end_it_cherry = skiplist.end_preffix("cherry");
  EXPECT_TRUE(end_it_cherry.is_end()); // 'cherry'是最后的key，所以结束是end()

  // 对于不存在的前缀，begin和end应该相同
  EXPECT_EQ(skiplist.begin_preffix("not exist"),
            skiplist.end_preffix("not exist"));
}

// 测试谓词迭代器
TEST(MySkiplistTest, ItersPredicate) {
  Skiplist skiplist;
  skiplist.put("prefix1", "value1", 10);
  skiplist.put("prefix2", "value2", 10);
  skiplist.put("prefix3", "value3", 10);
  skiplist.put("other", "value4", 10);
  skiplist.put("longerkey", "value5", 10);
  skiplist.put("medium", "value7", 10);
  skiplist.put("midway", "value8", 10);
  skiplist.put("midpoint", "value9", 10);
  
  // 测试范围匹配: key 在 ["medium", "midway"] 区间内
  auto range_result =
      skiplist.iters_monotony_predicate([](const std::string &key) {
        if (key < "medium") return 1;    // key太小，向右找
        if (key > "midway") return -1;   // key太大，向左找
        return 0;                        // 命中区间
      });
      
  ASSERT_TRUE(range_result.has_value());
  auto [begin_it, end_it] = range_result.value();
  
  std::vector<std::string> found_keys;
  for (auto it = begin_it; it != end_it; ++it) {
      found_keys.push_back(it.get_key());
  }
  
  std::vector<std::string> expected_keys = {"medium", "midpoint", "midway"};
  ASSERT_EQ(found_keys, expected_keys);
  EXPECT_EQ(end_it.get_key(), "other"); // 结束迭代器应指向区间的下一个元素
}

// 测试大规模数据下的谓词迭代器
TEST(MySkiplistTest, ItersPredicateLarge) {
  Skiplist skiplist;
  const int num_elements = 2000;
  
  for (int i = 0; i < num_elements; ++i) {
    std::ostringstream oss_key;
    oss_key << "key" << std::setw(4) << std::setfill('0') << i;
    skiplist.put(oss_key.str(), "v", 10);
  }

  // 移除一个元素以测试不连续性
  skiplist.remove("key1015");

  auto result = skiplist.iters_monotony_predicate([](const std::string &key) {
    if (key < "key1010") return 1;
    if (key >= "key1020") return -1;
    return 0;
  });

  ASSERT_TRUE(result.has_value());
  auto [begin_it, end_it] = result.value();
  
  EXPECT_EQ(begin_it.get_key(), "key1010");
  EXPECT_EQ(end_it.get_key(), "key1020");
  
  int count = 0;
  for(auto it = begin_it; it != end_it; ++it) {
      count++;
  }
  // 区间 [1010, 1020) 包含10个元素，移除了1个，所以是9个
  EXPECT_EQ(count, 9);
}


// 测试包含事务 id 的插入和查找
TEST(MySkiplistTest, TransactionId) {
  Skiplist skiplist;
  skiplist.put("key1", "value1_txn10", 10);
  skiplist.put("key1", "value2_txn20", 20);

  // 验证事务 id
  // 不指定事务 id (tranction_id=0)，应该返回最新的值 (txn_id最大的)
  EXPECT_EQ((skiplist.get("key1", 0).get_value()), "value2_txn20");
  
  // 指定一个看不到任何版本的事务id
  EXPECT_FALSE(skiplist.get("key1", 5).is_valid());

  // 指定 15 表示只能查找事务 id 小于等于 15 的值
  EXPECT_EQ((skiplist.get("key1", 15).get_value()), "value1_txn10");

  // 指定 25 表示能查到最新的值
  EXPECT_EQ((skiplist.get("key1", 25).get_value()), "value2_txn20");
}


// GTest的main函数
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // 如果你有日志或其他需要全局初始化的东西，可以在这里调用
  // 比如：init_spdlog_file();
  return RUN_ALL_TESTS();
}