#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <shared_mutex>
#include <mutex>
static std::shared_mutex mtx;

template <typename K, typename V> class Node {
public:
  Node(K k, V v, int level);
  ~Node();
  K get_key() const;
  V get_value() const;
  void set_value(V);

  Node<K, V> *
      *forward; // Linear array to hold pointers to next node of different level
  int node_level;

private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(K k, V v, int level) : key(k), value(v), node_level(level) {
  forward = new Node<K, V> *[level + 1];
  memset(forward, 0, sizeof(Node<K, V> *) * (level + 1));
}

template <typename K, typename V> Node<K, V>::~Node() { delete[] forward; }

template <typename K, typename V> K Node<K, V>::get_key() const { return key; }

template <typename K, typename V> V Node<K, V>::get_value() const {
  return value;
}

template <typename K, typename V> void Node<K, V>::set_value(V val) {
  value = val;
}

template <typename K, typename V> class SkipList {
public:
  SkipList(int max_level);
  ~SkipList();

  int insert_element(K key, V value);
  bool search_element(K key);
  void remove_element(K key);
  void display_list();
  int size() const;

private:
  int random_level();
  Node<K, V> *create_node(K key, V value, int level);
  void clear();

private:
  int _max_level;       // Maximum level of the skip list
  int _skip_list_level; // Current level of skip list
  Node<K, V> *_header;  // Pointer to header node
  int _element_count;   // Skiplist current element count
};

template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level)
    : _max_level(max_level), _skip_list_level(0), _element_count(0) {
  K k;
  V v;
  _header = new Node<K, V>(k, v, max_level);
}

template <typename K, typename V> 
SkipList<K, V>::~SkipList() {
  clear();
  delete _header;
}
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(K key, V value, int level) {
  Node<K, V> *new_node = new Node<K, V>(key, value, level);
  return new_node;
}

template <typename K, typename V> int SkipList<K, V>::random_level() {
  int level = 0;
  while (rand() % 2 && level < _max_level) {
    level++;
  }
  return level;
}

template <typename K, typename V>
int SkipList<K, V>::insert_element(K key, V value) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  Node<K, V> *current = _header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }
  current = current->forward[0];
  if (current && current->get_key() == key) {
    std::cout << "Key: " << key << " already exists." << std::endl;
    return 0; // Key already exists
  }
  int level = random_level();
  if (level > _skip_list_level) {
    for (int i = _skip_list_level + 1; i <= level; i++) {
      update[i] = _header;
    }
    _skip_list_level = level;
  }
  Node<K, V> *new_node = create_node(key, value, level);
  for (int i = 0; i <= level; i++) {
    new_node->forward[i] = update[i]->forward[i];
    update[i]->forward[i] = new_node;
  }
  _element_count++;
  std::cout << "Inserted key: " << key << ", value: " << value
            << " at level: " << level << std::endl;
  return 1; // Successfully inserted
}

template <typename K, typename V> 
bool SkipList<K, V>::search_element(K key) {
  std::shared_lock<std::shared_mutex> lock(mtx);

  Node<K, V> *current = _header;
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
  }
  current = current->forward[0];
  if (current && current->get_key() == key) {
    std::cout << "Found key: " << key << ", value: " << current->get_value()
              << std::endl;
    return true; // Key found
  }
  std::cout << "Not Found Key: " << key << std::endl;
  return false; // Key not found
}

template <typename K, typename V> 
void SkipList<K, V>::remove_element(K key) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  Node<K, V> *current = _header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }
  current = current->forward[0];
  if (current && current->get_key() == key) {
    for (int i = 0; i <= _skip_list_level; i++) {
      if (update[i]->forward[i] != current) {
        break;
      }
      update[i]->forward[i] = current->forward[i];
    }
    while (_skip_list_level > 0 &&
           _header->forward[_skip_list_level] == nullptr) {
      _skip_list_level--;
    }
    std::cout << "Successfully deleted key: " << key << std::endl;
    delete current;
    _element_count--;
  } else {

    std::cout << "Key: " << key << " not found for deletion." << std::endl;
  }
}

template <typename K, typename V> 
void SkipList<K, V>::display_list() {
  std::shared_lock<std::shared_mutex> lock(mtx);
  for (int i = 0; i <= _skip_list_level; i++) {
    Node<K, V> *current = _header->forward[i];
    std::cout << "Level " << i << ": ";
    while (current) {
      std::cout << "(" << current->get_key() << ", " << current->get_value()
                << ") ";
      current = current->forward[i];
    }
    std::cout << std::endl;
  }
}

template <typename K, typename V>
int SkipList<K, V>::size() const {
  std::shared_lock<std::shared_mutex> lock(mtx);
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::clear() {
    std::unique_lock<std::shared_mutex> lock(mtx);
    Node<K, V> *current = _header->forward[0];
    while (current) {       
        Node<K, V> *next = current->forward[0];
        delete current;
        current = next;
    }
    _header->forward[0] = nullptr;
    _skip_list_level = 0;
    _element_count = 0;
    std::cout << "SkipList cleared." << std::endl;
}
#endif