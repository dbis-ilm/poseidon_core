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

#include <set>

#include "config.h"
#include "graph_db.hpp"
#include "graph_pool.hpp"
#include "qop.hpp"
#include <catch2/catch_test_macros.hpp>

const std::string test_path = PMDK_PATH("index_tst");
#if 0
TEST_CASE("Creating an index on nodes", "[index]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_idx_graph");

  graph->run_transaction([&]() {
    for (int i = 0; i < 100; i++) {
      auto id = graph->add_node("Person",
                              {{"name", std::any(std::string("John Doe"))},
                               {"age", std::any(42)},
                               {"id", std::any(i)},
                               {"dummy1", std::any(std::string("Dummy"))},
                               {"dummy2", std::any(1.2345)}},
                              true);
    }
    return true;
  });

  index_id idx; 
  graph->run_transaction([&]() {
    idx = graph->create_index("Person", "id");
    return true;
  });
  
  graph->begin_transaction();

  REQUIRE(graph->has_index("Person", "id"));
  CHECK_THROWS_AS(graph->get_index("Actor", "id"), unknown_index);

  bool found = false;
  graph->index_lookup(idx, 55u, [&found](auto& n) {
    found = true;
  });
  REQUIRE(found);

  graph->commit_transaction();

  graph_pool::destroy(pool);
}
#endif

TEST_CASE("Creating and restoring an index on nodes", "[index]") {
  {
    auto pool = graph_pool::create(test_path);
    auto graph = pool->create_graph("my_idx_graph2");

    graph->run_transaction([&]() {
      for (int i = 0; i < 100; i++) {
        graph->add_node("Person",
                        {{"name", std::any(std::string("John Doe"))},
                         {"age", std::any(42)},
                         {"id", std::any(i)},
                         {"dummy1", std::any(std::string("Dummy"))},
                         {"dummy2", std::any(1.2345)}},
                        true);
      }
      return true;
    });

    index_id idx;
    graph->run_transaction([&]() {
      spdlog::info("create index Person.id");
      idx = graph->create_index("Person", "id");
      return true;
    });
    pool->close();
  }
  {
    spdlog::info("----------- reopen graph pool -----------");
    auto pool = graph_pool::open(test_path);
    auto graph = pool->open_graph("my_idx_graph2");

    graph->begin_transaction();

    index_id idx;
    REQUIRE_NOTHROW(idx = graph->get_index("Person", "id"));
    CHECK_THROWS_AS(graph->get_index("Actor", "id"), unknown_index);

    bool found = false;
    graph->index_lookup(idx, 55u, [&found](auto &n) { found = true; });
    REQUIRE(found);

    graph->commit_transaction();
    graph_pool::destroy(pool);
  }
}

TEST_CASE("Creating and updating an index on nodes", "[index]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_idx_graph3");

  graph->run_transaction([&]() {
    for (int i = 0; i < 100; i++) {
      graph->add_node("Person",
                      {{"name", std::any(std::string("John Doe"))},
                       {"age", std::any(42)},
                       {"id", std::any(i)},
                       {"dummy1", std::any(std::string("Dummy"))},
                       {"dummy2", std::any(1.2345)}},
                      true);
    }
    return true;
  });

  index_id idx;
  graph->run_transaction([&]() {
    spdlog::info("create index Person.id");
    idx = graph->create_index("Person", "id");
    return true;
  });

  // create another person
  graph->run_transaction([&]() {
    graph->add_node("Person",
                    {{"name", std::any(std::string("Jane Roe"))},
                     {"age", std::any(66)},
                     {"id", std::any(1000)},
                     {"dummy1", std::any(std::string("Dummy"))},
                     {"dummy2", std::any(2.345)}},
                    true);
    return true;
  });

  graph->run_transaction([&]() {
    bool found = false;
    graph->index_lookup(idx, 1000, [&](auto &n) { found = true; });
    REQUIRE(found);
    return true;
  });

  // delete a person
  graph->run_transaction([&]() {
    graph->delete_node(55);
    return true;
  });

  graph->run_transaction([&]() {
    bool found = false;
    graph->index_lookup(idx, 55, [&](auto &n) { found = true; });
    REQUIRE(!found);
    return false;
  });

  // update a person
  graph->run_transaction([&]() {
    auto &n = graph->node_by_id(70);
    graph->update_node(n, // update
                       {
                           {"id", std::any(1100)},
                       },
                       "Person");
    return true;
  });

  graph->run_transaction([&]() {
    bool found = false;
    graph->index_lookup(idx, 1100, [&](auto &n) { found = true; });
    REQUIRE(found);
    return true;
  });

  graph->run_transaction([&]() {
    bool found = false;
    graph->index_lookup(idx, 70, [&](auto &n) { found = true; });
    REQUIRE(!found);
    return false;
  });

  graph_pool::destroy(pool);
}