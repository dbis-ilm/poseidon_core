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

#include "catch.hpp"
#include "qop.hpp"
#include "query.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create("/mnt/pmem0/poseidon/query_test", "",
                                    PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

void create_data(graph_db_ptr graph) {
#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto n7 =
      graph->add_node("Node", {{"id", boost::any(7)},
                               {"name", boost::any(std::string("aaa7"))},
                               {"other", boost::any(std::string("BBB7"))}});
  auto n6 =
      graph->add_node("Node", {{"id", boost::any(6)},
                               {"name", boost::any(std::string("aaa6"))},
                               {"other", boost::any(std::string("BBB6"))}});
  auto n5 =
      graph->add_node("Node", {{"id", boost::any(5)},
                               {"name", boost::any(std::string("aaa5"))},
                               {"other", boost::any(std::string("BBB5"))}});
  auto n4 =
      graph->add_node("Node", {{"id", boost::any(4)},
                               {"name", boost::any(std::string("aaa4"))},
                               {"other", boost::any(std::string("BBB4"))}});
  auto n3 =
      graph->add_node("Node", {{"id", boost::any(3)},
                               {"name", boost::any(std::string("aaa3"))},
                               {"other", boost::any(std::string("BBB3"))}});
  auto n2 =
      graph->add_node("Node", {{"id", boost::any(2)},
                               {"name", boost::any(std::string("aaa2"))},
                               {"other", boost::any(std::string("BBB2"))}});
  auto n1 =
      graph->add_node("Node", {{"id", boost::any(1)},
                               {"name", boost::any(std::string("aaa1"))},
                               {"other", boost::any(std::string("BBB1"))}});

#ifdef USE_TX
  graph->commit_transaction();
#endif
}

void create_join_data(graph_db_ptr graph) {
#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto n1 = graph->add_node("Node1", {{"id", boost::any(1)}});
  auto n2 = graph->add_node("Node1", {{"id", boost::any(2)}});
  auto n3 = graph->add_node("Node2", {{"id", boost::any(3)}});
  auto n4 = graph->add_node("Node2", {{"id", boost::any(4)}});

#ifdef USE_TX
  graph->commit_transaction();
#endif
}

graph_db_ptr create_graph(
#ifdef USE_PMDK
    nvm::pool_base &pop
#endif
) {
#ifdef USE_PMDK
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif
  return graph;
}

TEST_CASE("Testing query operators", "[qop]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif
  create_data(graph);

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  namespace pj = builtin;

  SECTION("limit") {
    result_set rs, expected;
    auto q = query(graph).all_nodes("Node").limit(3).collect(rs);
    q.start();

    rs.wait();

    REQUIRE(rs.data.size() == 3);
  }

  SECTION("order by") {
    result_set rs, expected;
    auto q = query(graph)
                 .all_nodes("Node")
                 .project({PExpr_(0, pj::int_property(res, "id"))})
                 .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                   return boost::get<int>(qr1[0]) < boost::get<int>(qr2[0]);
                 })
                 .collect(rs);
    q.start();

    rs.wait();
    for (int i = 1; i <= 7; i++) {
      expected.data.push_back({query_result(std::to_string(i))});
    }
    REQUIRE(rs == expected);
  }

  SECTION("has string property") {
    result_set rs, expected;
    auto dc = graph->get_code("aaa4");
    REQUIRE(dc != 0);
    auto q = query(graph)
                 .all_nodes("Node")
                 .property("name", [dc](auto &p) { return p.equal(dc); })
                 .project({PExpr_(0, pj::int_property(res, "id")),
                           PExpr_(0, pj::string_property(res, "name"))})
                 .collect(rs);
    q.start();

    rs.wait();
    expected.append({query_result(std::to_string(4)), query_result("aaa4")});
    REQUIRE(rs == expected);
  }
#ifdef USE_TX
  graph->abort_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove("/mnt/pmem0/poseidon/query_test");
#endif
}

TEST_CASE("Testing join operators", "[qop]") {
  // TODO: prepare some data
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif
  create_join_data(graph);

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  namespace pj = builtin;

  SECTION("cross join") {
    result_set rs, expected;
    auto q1 = query(graph).all_nodes("Node2");

    auto q2 = query(graph)
                  .all_nodes("Node1")
                  .crossjoin(q1)
                  .project({PExpr_(0, pj::int_property(res, "id")),
                            PExpr_(1, pj::int_property(res, "id"))})
                  .collect(rs);
    query::start({&q1, &q2});

    rs.wait();
    expected.data.push_back({query_result("1"), query_result("3")});
    expected.data.push_back({query_result("1"), query_result("4")});
    expected.data.push_back({query_result("2"), query_result("3")});
    expected.data.push_back({query_result("2"), query_result("4")});
    REQUIRE(rs == expected);
  }

#ifdef USE_TX
  graph->abort_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove("/mnt/pmem0/poseidon/query_test");
#endif
}
