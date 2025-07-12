#include "SkipList.h"
#include <string>

int main() {
    SkipList<int, std::string> list(5);

    list.insert_element(3, "three");
    list.insert_element(6, "six");
    list.insert_element(1, "one");
    list.insert_element(7, "seven");

    list.display_list();

    list.search_element(3);
    list.search_element(4);

    list.remove_element(6);
    list.display_list();

    std::cout << "Total elements: " << list.size() << std::endl;

    // list.clear();
    list.display_list();

    return 0;
}
