/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <filesystem>
#include "fmt/format.h"
#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "defs.hpp"
#include "dict.hpp"

void create_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj))
        std::filesystem::create_directory(path_obj);
}

void delete_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    std::filesystem::remove_all(path_obj);
}

TEST_CASE("Inserting some strings", "[dict]") {
  create_dir("dict1");
  bufferpool bpool;
  dict d(bpool, "dict1");

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #4");
  d.insert("String #5");
  d.insert("String #1");
  d.insert("String #2");
  REQUIRE(d.size() == 5);

  delete_dir("dict1");
}

TEST_CASE("Inserting duplicate strings", "[dict]") {
  create_dir("dict2");
  bufferpool bpool;
  dict d(bpool, "dict2");

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #3");
  d.insert("String #1");
  REQUIRE(d.size() == 3);

  delete_dir("dict2");  
}

TEST_CASE("Looking up some strings", "[dict]") {
  create_dir("dict3");
  bufferpool bpool;
  dict d(bpool, "dict3");

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  auto c2 = d.insert("String #2");
  d.insert("String #3");
  auto c4 = d.insert("String #4");
  d.insert("String #5");

  REQUIRE(d.lookup_string("String #4") == c4);
  REQUIRE(d.lookup_string("String #2") == c2);

  delete_dir("dict3"); 
}

TEST_CASE("Looking up some codes", "[dict]") {
create_dir("dict4");
  bufferpool bpool;
  dict d(bpool, "dict4");

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  d.insert("String #2");
  auto c3 = d.insert("String #3");
  auto c4 = d.insert("String #4");
  d.insert("String #5");

  REQUIRE(std::string("String #4") == d.lookup_code(c4));
  REQUIRE(std::string("String #3") ==d.lookup_code(c3));

  delete_dir("dict4"); 
}

TEST_CASE("Looking up some non-existing strings", "[dict]") {
  create_dir("dict5");
  bufferpool bpool;
  dict d(bpool, "dict5");

  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #4");
  d.insert("String #5");
  REQUIRE(d.lookup_string("Unknown string") == 0);

  delete_dir("dict5"); 
}

TEST_CASE("Test persistency of dict", "[dict]") {
  dcode_t c;
  create_dir("dict6");
  {
    bufferpool bpool;
    dict d(bpool, "dict6");

    d.insert("String #1");
    d.insert("String #2");
    d.insert("String #3");
    c = d.insert("String #4");
    d.insert("String #5");

  }

  {
    bufferpool bpool;
    dict d2(bpool, "dict6");

    REQUIRE(d2.lookup_string("String #4") == c);
  }
  delete_dir("dict6");
}

// * test with a large set of strings
TEST_CASE("Inserting many items", "[dict]") {
  {
    create_dir("dict7");
    bufferpool bpool;
    dict d(bpool, "dict7");

    std::cout << "dict7...insert" << std::endl;
  // max: 4294967295
  // for (uint64_t i = 0u; i < 10000000; i++) {
    for (uint64_t i = 0u; i < 100000; i++) {
//	std::cout << i << std::endl;
      d.insert(fmt::format("DictEntry#{}", i));
    }

    std::cout << "...lookup" << std::endl;
    for (auto i = 1000u; i < 100000; i++) {
      auto str = fmt::format("DictEntry#{}", i);
      auto c = d.lookup_string(str);
      REQUIRE(c != 0);
    }
    std::cout << "finished." << std::endl;
    d.print_table();
  }
  {
    std::cout << "restart...." << std::endl;
    bufferpool bpool;
    dict d(bpool, "dict7");
    for (auto i = 1000u; i < 100000; i++) {
      auto str = fmt::format("DictEntry#{}", i);
      // std::cout << str << std::endl;
      auto c = d.lookup_string(str);
      REQUIRE(c != 0);
    }
  }
  delete_dir("dict7"); 
}
