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

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "nodes.hpp"

#include <sstream>
#include <filesystem>

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

TEST_CASE("Testing output functions", "[nodes]") {
  {
    std::ostringstream os;
    std::any v(12);
    os << v;
    REQUIRE(os.str() == "12");
  }
  {
    std::ostringstream os;
    std::any v(12.34);
    os << v;
    REQUIRE(os.str() == "12.34");
  }
  {
    std::ostringstream os;
    std::any v(true);
    os << v;
    REQUIRE(os.str() == "1");
  }
  {
    std::ostringstream os;
    std::any v((uint64_t)1234);
    os << v;
    REQUIRE(os.str() == "1234");
  }
  {
    std::ostringstream os;
    boost::posix_time::ptime pt{ boost::gregorian::date{2014, 5, 12}, 
      boost::posix_time::time_duration{12, 0, 0}};
    std::any v(pt);
    os << v;
    REQUIRE(os.str() == "2014-May-12 12:00:00");
  }
}

TEST_CASE("Creating a node", "[nodes]") {
  node n(42);
  REQUIRE(n.node_label == 42);
  REQUIRE(n.from_rship_list == UNKNOWN);
  REQUIRE(n.to_rship_list == UNKNOWN);
  REQUIRE(n.property_list == UNKNOWN);
  REQUIRE(n.id() == UNKNOWN);
  n.property_list = 66;
  REQUIRE(n.property_list == 66);
}

// ----------------------------------------------------------------------------
//       node_list with buffered_vec
// ----------------------------------------------------------------------------
TEST_CASE("Creating a few nodes in the pfile_node_list", "[pfile_node_list]") {
  create_dir("my_ntest1");

  SECTION("Creating nodes") {
  auto test_file = std::make_shared<paged_file>();
  test_file->open("my_ntest1/nodes1.db", 1);
  bufferpool bpool;
  bpool.register_file(1, test_file);

  node_list<buffered_vec> nlist(bpool, 1);

  auto n1 = nlist.add(node(62));
  auto n2 = nlist.add(node(63));
  auto n3 = nlist.add(node(64));

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  REQUIRE(nlist.get(n2).id() == n2);
  REQUIRE(nlist.get(n2).node_label == 63);

  REQUIRE(nlist.get(n3).id() == n3);
  REQUIRE(nlist.get(n3).node_label == 64);

  REQUIRE(nlist.num_chunks() == 1);

  CHECK_THROWS_AS(nlist.get(47), unknown_id);
  CHECK_THROWS_AS(nlist.get(10000), unknown_id);

  nlist.remove(n3);
  CHECK_THROWS_AS(nlist.get(n3), unknown_id);
  // if the capacity is larger than 10000 then no exception is raised
  // CHECK_THROWS_AS(nlist.remove(10000), unknown_id); 
  }
  delete_dir("my_ntest1");
}
 
TEST_CASE("Creating and restoring a persistent pfile_node_list", "[pfile_node_list]") {
  node::id_t n1 = 0, n2 = 0, n3 = 0;
  {
  create_dir("my_ntest2");
  auto test_file = std::make_shared<paged_file>();
  test_file->open("my_ntest2/nodes2.db", 1);
  bufferpool bpool;
  bpool.register_file(1, test_file);

  node_list<buffered_vec> nlist(bpool, 1);

  n1 = nlist.add(node(62));
  n2 = nlist.add(node(63));
  n3 = nlist.add(node(64));

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  REQUIRE(nlist.get(n2).id() == n2);
  REQUIRE(nlist.get(n2).node_label == 63);

  REQUIRE(nlist.get(n3).id() == n3);
  REQUIRE(nlist.get(n3).node_label == 64);

  nlist.dump();
  }

  { 
  auto test_file = std::make_shared<paged_file>();
  test_file->open("my_ntest2/nodes2.db", 1);
  bufferpool bpool;
  bpool.register_file(1, test_file);

  node_list<buffered_vec> nlist2(bpool, 1);

  nlist2.dump();

  REQUIRE(nlist2.get(n1).id() == n1);
  REQUIRE(nlist2.get(n1).node_label == 62);

  REQUIRE(nlist2.get(n2).id() == n2);
  REQUIRE(nlist2.get(n2).node_label == 63);

  REQUIRE(nlist2.get(n3).id() == n3);
  REQUIRE(nlist2.get(n3).node_label == 64);

  }
  delete_dir("my_ntest2");
}

TEST_CASE("Deleting a node in a pfile_node_list", "[pfile_node_list]") {
  create_dir("my_ntest3");
  auto test_file = std::make_shared<paged_file>();
  test_file->open("my_ntest3/nodes3.db", 1);
  bufferpool bpool;
  bpool.register_file(1, test_file);

  node_list<buffered_vec> nlist(bpool, 1);

  auto n1 = nlist.add(node(62));
  auto n2 = nlist.add(node(63));
  auto n3 = nlist.add(node(64));

  nlist.remove(n2);

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  REQUIRE(nlist.get(n3).id() == n3);
  REQUIRE(nlist.get(n3).node_label == 64);

  // nlist.get(n2) should raise an exception
  REQUIRE_THROWS_AS(nlist.get(n2), unknown_id);

  delete_dir("my_ntest3");
}

TEST_CASE("Appending a node to a pfile_node_list", "[pfile_node_list]") {
  create_dir("my_ntest4");
  auto test_file = std::make_shared<paged_file>();
  test_file->open("my_ntest4/nodes4.db", 1);
  bufferpool bpool;
  bpool.register_file(1, test_file);

  node_list<buffered_vec> nlist(bpool, 1);

  auto n1 = nlist.append(node(62));
  REQUIRE(n1 == 0);

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  for (auto i = 0u; i < 100; i++)
    nlist.append(node(100 + i));

  int num = 0;
  for (auto& ni : nlist.as_vec()) {
    num++;
  }
  REQUIRE(num == 101);
  
  delete_dir("my_ntest4");
}

TEST_CASE("Checking reuse of space in a pfile_node_list", "[pfile_node_list]") {
  // TODO
}

