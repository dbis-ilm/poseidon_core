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
#include "qop.hpp"
#include "query_builder.hpp"
#include "graph_pool.hpp"

using namespace boost::posix_time;
const std::string test_path = PMDK_PATH("path_tst");

TEST_CASE("Finding Unweighted Shortest Path", "[shortest_path]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_pgraph1");
  query_ctx ctx(graph);

  path_item ss_paths;
  path_visitor path_vis = [&](node &n, const path &p) { return; };
  auto rpred = [&](relationship &r) {
                return std::string(graph->get_string(r.rship_label)) == ":knows"; };

  ctx.begin_transaction();

  auto a = graph->add_node(":Person", {{"name",
            std::any(std::string("John"))}, {"age", std::any(42)}});
  auto b = graph->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                {"age", std::any(36)}});
  auto c = graph->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                {"age", std::any(58)}});
  auto d = graph->add_node(":Person", {{"name", std::any(std::string("Han"))},
                                {"age", std::any(13)}});
  auto e = graph->add_node(":Person", {{"name", std::any(std::string("Zaki"))},
                                {"age", std::any(47)}});

  graph->add_relationship(a, b, ":knows", {});
  graph->add_relationship(b, c, ":knows", {});
  graph->add_relationship(c, d, ":knows", {});
  graph->add_relationship(a, e, ":knows", {});
  graph->add_relationship(d, e, ":knows", {});

  std::vector<uint64_t> exp_path = {0, 4, 3};
  bool found = unweighted_shortest_path(ctx, a, d, true, rpred, path_vis, ss_paths);

  REQUIRE(found);
  REQUIRE(ss_paths.get_hops() == 2);
  REQUIRE(ss_paths.get_path() == exp_path);

  ctx.commit_transaction();

  graph_pool::destroy(pool);
}

TEST_CASE("Weighted Shortest Path", "[shortest_path]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_pgraph2");
  query_ctx ctx(graph);

  path_item ss_path;
  path_visitor path_vis = [&](node &n, const path &p) { return; };
  auto rpred = [&](relationship &r) {
                return std::string(graph->get_string(r.rship_label)) == ":knows"; };

  auto rweight = [&](relationship &r) {
        // auto &src = graph->node_by_id(r.from_node_id());
        // auto &des = graph->node_by_id(r.to_node_id());
        auto src_descr = graph->get_node_description(r.from_node_id());
        auto des_descr = graph->get_node_description(r.to_node_id());
        auto src_age = get_property<int>(src_descr.properties, 
                                      std::string("age")).value();
        auto des_age = get_property<int>(des_descr.properties, 
                                      std::string("age")).value();
        return (double)(src_age + des_age); };

  ctx.begin_transaction();

  auto a = graph->add_node(":Person", {{"name",
            std::any(std::string("John"))}, {"age", std::any(42)}});
  auto b = graph->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                {"age", std::any(36)}});
  auto c = graph->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                {"age", std::any(58)}});
  auto d = graph->add_node(":Person", {{"name", std::any(std::string("Han"))},
                                {"age", std::any(13)}});
  auto e = graph->add_node(":Person", {{"name", std::any(std::string("Zaki"))},
                                {"age", std::any(47)}});

  graph->add_relationship(a, b, ":knows", {});
  graph->add_relationship(b, c, ":knows", {});
  graph->add_relationship(c, d, ":knows", {});
  graph->add_relationship(a, e, ":knows", {});
  graph->add_relationship(d, e, ":knows", {});

  weighted_shortest_path(ctx, a, d, true, rpred, rweight, path_vis, ss_path);

  REQUIRE(ss_path.get_weight() == 149.0);

  ctx.commit_transaction();

  graph_pool::destroy(pool);
}

TEST_CASE("Top K Weighted Shortest Paths", "[shortest_path]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_pgraph3");
  query_ctx ctx(graph);

  std::size_t k = 2;
  std::vector<path_item> k_spath;
  path_visitor path_vis = [&](node &n, const path &p) { return; };
  auto rpred = [&](relationship &r) {
                return std::string(graph->get_string(r.rship_label)) == ":knows"; };

  auto rweight = [&](relationship &r) {
        // auto &src = graph->node_by_id(r.from_node_id());
        // auto &des = graph->node_by_id(r.to_node_id());
        auto src_descr = graph->get_node_description(r.from_node_id());
        auto des_descr = graph->get_node_description(r.to_node_id());
        auto src_age = get_property<int>(src_descr.properties, 
                                      std::string("age")).value();
        auto des_age = get_property<int>(des_descr.properties, 
                                      std::string("age")).value();
        return (double)(src_age + des_age); };

  ctx.begin_transaction();

  auto a = graph->add_node(":Person", {{"name",
            std::any(std::string("John"))}, {"age", std::any(42)}});
  auto b = graph->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                {"age", std::any(36)}});
  auto c = graph->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                {"age", std::any(58)}});
  auto d = graph->add_node(":Person", {{"name", std::any(std::string("Han"))},
                                {"age", std::any(13)}});
  auto e = graph->add_node(":Person", {{"name", std::any(std::string("Zaki"))},
                                {"age", std::any(47)}});
  auto f = graph->add_node(":Person", {{"name", std::any(std::string("Zaki"))},
                                {"age", std::any(81)}});
  auto g = graph->add_node(":Person", {{"name", std::any(std::string("Zaki"))},
                                {"age", std::any(23)}});

  graph->add_relationship(a, b, ":knows", {});
  graph->add_relationship(b, c, ":knows", {});
  graph->add_relationship(c, d, ":knows", {});
  graph->add_relationship(e, d, ":knows", {});
  graph->add_relationship(f, e, ":knows", {});
  graph->add_relationship(a, g, ":knows", {});
  graph->add_relationship(g, f, ":knows", {});

  k_weighted_shortest_path(ctx, a, d, k, true, rpred, rweight, path_vis, k_spath);

  REQUIRE(k_spath.size() == 2);

  ctx.commit_transaction();

  graph_pool::destroy(pool);
}
