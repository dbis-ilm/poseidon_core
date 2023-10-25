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
#include "query_builder.hpp"
#include "graph_pool.hpp"
#include "bfs.hpp"

const std::string test_path = PMDK_PATH("graphalytics_tst");

void create_data(graph_db_ptr graph) {
  graph->begin_transaction();

  /**
   *  0 --> 1 --> 2 --> 3 --> 4
   *  |           ^
   *  |           |
   *  +---------> 6
   *              ^
   *              |
   *              5
  */

  for (int i = 0; i < 7; i++) {
    graph->add_node("Person",
                                  {{"name", std::any(std::string("John Doe"))},
                                  {"age", std::any(42)},
                                  {"id", std::any(i)},
                                  {"dummy1", std::any(std::string("Dummy"))},
                                  {"dummy2", std::any(1.2345)}},
                                  true);
  }
  graph->add_relationship(0, 1, ":knows", {});
  graph->add_relationship(1, 2, ":knows", {});
  graph->add_relationship(2, 3, ":knows", {});
  graph->add_relationship(3, 4, ":knows", {});
  graph->add_relationship(5, 6, ":knows", {});
  graph->add_relationship(6, 2, ":knows", {});
  graph->add_relationship(0, 6, ":likes", {});
  graph->commit_transaction();
}

TEST_CASE("Sequential BFS", "[ldbc]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_ga_graph1");
  create_data(graph);

  graph->dump_dot("bfs-graph.dot");
  query_ctx ctx(graph);

  SECTION("bidirectional test") {
    std::vector<int> rs, expected;

    graph->begin_transaction(); 
    bfs(ctx, 0, false, 
        [](auto& r) { return true; }, 
        [&](auto& n) { rs.push_back(n.id()); }
    );
    graph->commit_transaction();

    expected = { 0, 6, 1, 2, 3, 4 };
    REQUIRE(rs == expected);
  }

  SECTION("unidirectional test") {
    std::vector<int> rs, expected;

    graph->begin_transaction(); 
    bfs(ctx, 0, true, 
        [](auto& r) { return true; }, 
        [&](auto& n) { rs.push_back(n.id()); }
    );
    graph->commit_transaction();

    expected = { 0, 6, 1, 2, 5, 3, 4 };
    REQUIRE(rs == expected);
  }

  SECTION("relationship filter test") {
    std::vector<int> rs, expected;

    auto label = graph->get_code(":knows");
    graph->begin_transaction(); 
    bfs(ctx, 0, false, 
        [&](auto& r) { return r.rship_label == label; }, 
        [&](auto& n) { rs.push_back(n.id()); }
    );
    graph->commit_transaction();

    expected = { 0, 1, 2, 3, 4};
    REQUIRE(rs == expected);
  }

  SECTION("path visitor test") {
    std::vector<path> rs, expected;

    graph->begin_transaction(); 
    path_bfs(ctx, 0, false, 
        [&](auto& r) { return true; }, 
        [&](auto& n, const path& p) { 
          rs.push_back(p);
        }
    );
    graph->commit_transaction();

    expected = { { 0 }, { 0, 6 }, { 0, 1 }, { 0, 6, 2 }, { 0, 6, 2, 3 }, { 0, 6, 2, 3, 4 } };
    REQUIRE(rs == expected);
  }
  graph_pool::destroy(pool);
}
