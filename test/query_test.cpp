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
#include "qop_builtins.hpp"
#include "query_builder.hpp"
#include "graph_pool.hpp"

using namespace boost::posix_time;
const std::string test_path = PMDK_PATH("query_tst");

void create_data(graph_db_ptr graph) {
  graph->begin_transaction();

      graph->add_node("Node", {{"id", std::any(7)},
                               {"name", std::any(std::string("aaa7"))},
                               {"other", std::any(std::string("BBB7"))}});

      graph->add_node("Node", {{"id", std::any(6)},
                               {"name", std::any(std::string("aaa6"))},
                               {"other", std::any(std::string("BBB6"))}});

      graph->add_node("Node", {{"id", std::any(5)},
                               {"name", std::any(std::string("aaa5"))},
                               {"other", std::any(std::string("BBB5"))}});

      graph->add_node("Node", {{"id", std::any(4)},
                               {"name", std::any(std::string("aaa4"))},
                               {"other", std::any(std::string("BBB4"))}});

      graph->add_node("Node", {{"id", std::any(3)},
                               {"name", std::any(std::string("aaa3"))},
                               {"other", std::any(std::string("BBB3"))}});

      graph->add_node("Node", {{"id", std::any(2)},
                               {"name", std::any(std::string("aaa2"))},
                               {"other", std::any(std::string("BBB2"))}});

      graph->add_node("Node", {{"id", std::any(1)},
                               {"name", std::any(std::string("aaa1"))},
                               {"other", std::any(std::string("BBB1"))}});
  graph->commit_transaction();
}

void create_join_data(graph_db_ptr graph) {
  graph->begin_transaction();

  graph->add_node("Node1", {{"id", std::any(1)}});
  graph->add_node("Node1", {{"id", std::any(2)}});
  graph->add_node("Node2", {{"id", std::any(3)}});
  graph->add_node("Node2", {{"id", std::any(4)}});

  graph->commit_transaction();
}


TEST_CASE("Testing query operators", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph1");
  query_ctx ctx(graph);

  create_data(graph);

  ctx.begin_transaction();

  namespace pj = builtin;

  SECTION("limit") {
    result_set rs, expected;
    auto q = query_builder(ctx).all_nodes("Node").limit(3).collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();

    REQUIRE(rs.data.size() == 3);
    q.print_plan();
  }

  SECTION("count") {
    result_set rs, expected;
    auto q = query_builder(ctx).all_nodes("Node").count().collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.data.push_back({query_result("7")});

    REQUIRE(rs == expected);
    q.print_plan();
  }

  SECTION("order by") {
    result_set rs, expected;
    auto q = query_builder(ctx)
                 .all_nodes("Node")
                 .project({PExpr_(0, pj::int_property(res, "id"))})
                 .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                   return boost::get<int>(qr1[0]) < boost::get<int>(qr2[0]);
                 })
                 .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    for (int i = 1; i <= 7; i++) {
      expected.data.push_back({query_result(std::to_string(i))});
    }
    REQUIRE(rs == expected);
    q.print_plan();
  }

  SECTION("has string property") {
    result_set rs, expected;
    auto dc = graph->get_code("aaa4");
    REQUIRE(dc != 0);
    auto q = query_builder(ctx)
                 .all_nodes("Node")
                 .property("name", [dc](auto &p) { return p.equal(dc); })
                 .project({PExpr_(0, pj::int_property(res, "id")),
                           PExpr_(0, pj::string_property(res, "name"))})
                 .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.append({query_result(std::to_string(4)), query_result("aaa4")});
    REQUIRE(rs == expected);
    q.print_plan();
  }

  SECTION("has label") {
    graph->add_node("Movie", {{"title", std::any(std::string("m1"))}});
    graph->add_node("Movie", {{"title", std::any(std::string("m2"))}});
    graph->add_node("Actor", {{"name", std::any(std::string("p1"))}});
    graph->add_node("Actor", {{"name", std::any(std::string("p2"))}});
    // graph->dump();
    // std::cout << "code for Movie: " << graph->get_code("Movie") << std::endl;

    result_set rs, expected;
    auto q = query_builder(ctx)
                 .all_nodes()
                 .has_label("Movie")
                 .project({PExpr_(0, pj::string_property(res, "title"))})
                 .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.append({query_result("m1")});
    expected.append({query_result("m2")});
    q.print_plan();
    REQUIRE(rs == expected);
  }

  SECTION("node scan with label and property predicate") {
    result_set rs, expected;
    auto dc = graph->get_code("aaa4");
    REQUIRE(dc != 0);
    auto q = query_builder(ctx)
                 .nodes_where("Node", "name", [dc](auto &p) { return p.equal(dc); })
                 .project({PExpr_(0, pj::int_property(res, "id")),
                           PExpr_(0, pj::string_property(res, "name"))})
                 .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.append({query_result(std::to_string(4)), query_result("aaa4")});
    REQUIRE(rs == expected);
    q.print_plan();
  }

  SECTION("use index") {
    ctx.commit_transaction();
    ctx.begin_transaction();

    // create index
    auto idx = graph->create_index("Node", "id");
 
    result_set rs, expected;
    auto q = query_builder(ctx)
              .nodes_where_indexed("Node", "id", 3)
              .project({PExpr_(0, pj::int_property(res, "id")),
                        PExpr_(0, pj::string_property(res, "name"))})
              .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.append({query_result(std::to_string(3)), query_result("aaa3")});
    REQUIRE(rs == expected);
    q.print_plan();
  }

  SECTION("where_qr_tuple") {
    result_set rs, expected;
    auto q = query_builder(ctx)
                 .all_nodes("Node")
                 .project({PExpr_(0, pj::int_property(res, "id"))})
                 .where_qr_tuple([&](const auto &v) {
                   return boost::get<int>(v[0]) > 4; })
                 .collect(rs).get_pipeline();
    q.start(ctx);

    rs.wait();
    expected.append({query_result("7")});
    expected.append({query_result("6")});
    expected.append({query_result("5")});
    REQUIRE(rs == expected);
    q.print_plan();
  }
}

TEST_CASE("Testing join operators", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph2");
  query_ctx ctx(graph);

  // prepare some data
  create_join_data(graph);

  ctx.begin_transaction();

  namespace pj = builtin;

  SECTION("cross join") {
    result_set rs, expected;
    auto q1 = query_builder(ctx).all_nodes("Node2").get_pipeline();

    auto q2 = query_builder(ctx)
                  .all_nodes("Node1")
                  .cross_join(q1)
                  .project({PExpr_(0, pj::int_property(res, "id")),
                            PExpr_(1, pj::int_property(res, "id"))})
                  .collect(rs).get_pipeline();
    query_ctx::start(ctx, {&q1, &q2});

    rs.wait();
    expected.data.push_back({query_result("3"), query_result("1")});
    expected.data.push_back({query_result("4"), query_result("1")});
    expected.data.push_back({query_result("3"), query_result("2")});
    expected.data.push_back({query_result("4"), query_result("2")});
    REQUIRE(rs == expected);
    query_ctx::print_plans({&q1, &q2});
  }

  ctx.abort_transaction();

  graph_pool::destroy(pool);
}

TEST_CASE("Projecting node and relationship datetime properties", "[graph_db]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph3");
  query_ctx ctx(graph);

  ctx.begin_transaction();

  auto a = graph->add_node("Person",
      {{"id", std::any(1)},
      {"creationDate", std::any(time_from_string(std::string("2011-11-01 00:05:56.000")))}});
  auto b = graph->add_node("Place",
      {{"id", std::any(24)},
      {"creationDate", std::any(time_from_string(std::string("2010-06-10 11:50:26.000")))}});

  graph->add_relationship(a, b, ":isLocatedIn",
      {{"creationDate", std::any(time_from_string(std::string("2011-11-02 13:00:00.000")))}});

  ctx.commit_transaction();
  ctx.begin_transaction();

  namespace pj = builtin;
  result_set rs, expected;
  auto q = query_builder(ctx)
                  .all_nodes("Person")
                  .from_relationships(":isLocatedIn")
                  .to_node("Place")
                  .project({PExpr_(0, pj::ptime_property(res, "creationDate")),
                            PExpr_(2, pj::ptime_property(res, "creationDate")),
                            PExpr_(1, pj::ptime_property(res, "creationDate")) })
                  .collect(rs).get_pipeline();
    q.start(ctx);
    rs.wait();

  expected.data.push_back(
    {query_result("2011-11-01T00:05:56"),
    query_result("2010-06-10T11:50:26"),
    query_result("2011-11-02T13:00:00")});
  REQUIRE(rs == expected);

  ctx.commit_transaction();

  graph_pool::destroy(pool);
}

TEST_CASE("Testing query profiling", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph4");
  query_ctx ctx(graph);

  create_data(graph);
  auto dc = graph->get_code("aaa3");
  ctx.run_transaction([&]() {
    result_set rs;
    auto q = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(dc); })
              .collect(rs).get_pipeline();
    q.start(ctx);
    rs.wait();
    q.print_plan();
    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing union_all operator", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph5");
  query_ctx ctx(graph);

  namespace pj = builtin;

  create_data(graph);
  auto ab = graph->get_code("aaa3");
  auto cd = graph->get_code("aaa7");
  ctx.run_transaction([&]() {
    result_set rs, expected;
    expected.append({query_result("aaa3")});
    expected.append({query_result("aaa7")});

    auto q1 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(ab); })
              .project({PExpr_(0, pj::string_property(res, "name"))}).get_pipeline();
    
    auto q2 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(cd); })
              .project({PExpr_(0, pj::string_property(res, "name"))})
              .union_all(q1)
              .collect(rs).get_pipeline();

    query_ctx::start(ctx, {&q1, &q2});
    rs.wait();
    query_ctx::print_plans({&q1, &q2});

    REQUIRE(rs == expected);
    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing union_all operator 2", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph6");
  query_ctx ctx(graph);

  namespace pj = builtin;

  create_data(graph);
  auto a = graph->get_code("aaa1");
  auto b = graph->get_code("aaa2");
  auto c = graph->get_code("aaa3");
  auto d = graph->get_code("aaa4");
  ctx.run_transaction([&]() {
    result_set rs, expected;
    expected.append({query_result("aaa1")});
    expected.append({query_result("aaa2")});
    expected.append({query_result("aaa3")});
    expected.append({query_result("aaa4")});

    auto q1 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(a); })
              .project({PExpr_(0, pj::string_property(res, "name"))}).get_pipeline();

    auto q2 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(b); })
              .project({PExpr_(0, pj::string_property(res, "name"))}).get_pipeline();

    auto q3 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(c); })
              .project({PExpr_(0, pj::string_property(res, "name"))}).get_pipeline();
    
    auto q4 = query_builder(ctx)
              .all_nodes("Node")
              .property("name", [&](auto &p) { return p.equal(d); })
              .project({PExpr_(0, pj::string_property(res, "name"))})
              .union_all({&q1, &q2, &q3})
              .collect(rs).get_pipeline();

    query_ctx::start(ctx, {&q1, &q2, &q3, &q4});
    rs.wait();
    query_ctx::print_plans({&q1, &q2, &q3, &q4});

    REQUIRE(rs == expected);
    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing outgoing traversal operators", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph7");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    auto a = graph->add_node("Person", {{"id", std::any(1)},
                                        {"firstName", std::any(std::string("A"))}});
    auto b = graph->add_node("Person", {{"id", std::any(2)},
                                        {"firstName", std::any(std::string("B"))}});
    auto c = graph->add_node("Person", {{"id", std::any(3)},
                                        {"firstName", std::any(std::string("C"))}});
    auto d = graph->add_node("Person", {{"id", std::any(4)},
                                        {"firstName", std::any(std::string("D"))}});
    auto e = graph->add_node("Person", {{"id", std::any(5)},
                                        {"firstName", std::any(std::string("E"))}});
    auto f = graph->add_node("Person", {{"id", std::any(6)},
                                        {"firstName", std::any(std::string("F"))}});

    graph->add_relationship(a, b, ":knows", {});
    graph->add_relationship(a, c, ":knows", {});
    graph->add_relationship(a, d, ":knows", {});
    graph->add_relationship(b, e, ":knows", {});
    graph->add_relationship(e, f, ":knows", {});
    return true;
  });

  ctx.run_transaction([&]() {

    SECTION("foreach_from_relationship") {
      result_set rs, expected;
      auto q = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships(":knows")
                .to_node("Person")
                .project({PExpr_(2, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      q.start(ctx);
      rs.wait();
      q.print_plan();

      expected.data.push_back({query_result("D")});
      expected.data.push_back({query_result("C")});
      expected.data.push_back({query_result("B")});

      REQUIRE(rs == expected);
    }

    SECTION("foreach_variable_from_relationship") {
      result_set rs, expected;
      auto q = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships({1, 3}, ":knows")
                .to_node("Person")
                .project({PExpr_(2, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      q.start(ctx);
      rs.wait();
      q.print_plan();

      expected.data.push_back({query_result("D")});
      expected.data.push_back({query_result("C")});
      expected.data.push_back({query_result("B")});
      expected.data.push_back({query_result("E")});
      expected.data.push_back({query_result("F")});

      REQUIRE(rs == expected);
    }

    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing incoming traversal operators", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph8");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    auto a = graph->add_node("Person", {{"id", std::any(1)},
                                        {"firstName", std::any(std::string("A"))}});
    auto b = graph->add_node("Person", {{"id", std::any(2)},
                                        {"firstName", std::any(std::string("B"))}});
    auto c = graph->add_node("Person", {{"id", std::any(3)},
                                        {"firstName", std::any(std::string("C"))}});
    auto d = graph->add_node("Person", {{"id", std::any(4)},
                                        {"firstName", std::any(std::string("D"))}});
    auto e = graph->add_node("Person", {{"id", std::any(5)},
                                        {"firstName", std::any(std::string("E"))}});
    auto f = graph->add_node("Person", {{"id", std::any(6)},
                                        {"firstName", std::any(std::string("F"))}});

    graph->add_relationship(a, b, ":knows", {});
    graph->add_relationship(a, c, ":knows", {});
    graph->add_relationship(a, d, ":knows", {});
    graph->add_relationship(b, e, ":knows", {});
    graph->add_relationship(e, f, ":knows", {});
    return true;
  });

  ctx.run_transaction([&]() {

    SECTION("foreach_from_relationship") {
      result_set rs, expected;
      auto q = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("F")); })
                .to_relationships(":knows")
                .from_node("Person")
                .project({PExpr_(2, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      q.start(ctx);
      rs.wait();
      q.print_plan();

      expected.data.push_back({query_result("E")});

      REQUIRE(rs == expected);
    }

    SECTION("foreach_variable_from_relationship") {
      result_set rs, expected;
      auto q = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("F")); })
                .to_relationships({1, 3}, ":knows")
                .from_node("Person")
                .project({PExpr_(2, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      q.start(ctx);
      rs.wait();
      q.print_plan();

      expected.data.push_back({query_result("E")});
      expected.data.push_back({query_result("B")});
      expected.data.push_back({query_result("A")});

      REQUIRE(rs == expected);
    }

    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing other Join operators", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph9");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    auto a = graph->add_node("Person", {{"id", std::any(1)},
                                        {"firstName", std::any(std::string("A"))}});
    auto b = graph->add_node("Person", {{"id", std::any(2)},
                                        {"firstName", std::any(std::string("B"))}});
    auto c = graph->add_node("Person", {{"id", std::any(3)},
                                        {"firstName", std::any(std::string("C"))}});
    auto d = graph->add_node("Person", {{"id", std::any(4)},
                                        {"firstName", std::any(std::string("D"))}});
    auto e = graph->add_node("Person", {{"id", std::any(5)},
                                        {"firstName", std::any(std::string("E"))}});
    auto f = graph->add_node("Person", {{"id", std::any(6)},
                                        {"firstName", std::any(std::string("F"))}});

    graph->add_relationship(a, b, ":knows", {});
    graph->add_relationship(a, c, ":knows", {});
    graph->add_relationship(a, d, ":knows", {});
    graph->add_relationship(b, e, ":knows", {});
    graph->add_relationship(e, f, ":knows", {});
    return true;
  });

  ctx.run_transaction([&]() {

    SECTION("hashjoin_on_node") {
      result_set rs, expected;
      auto q1 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships(":knows")
                .to_node("Person").get_pipeline();

      auto q2 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships({1, 3}, ":knows")
                .to_node("Person")
                .hash_join({2, 2}, q1)
                .project({PExpr_(0, pj::string_property(res, "firstName")),
                          PExpr_(2, pj::string_property(res, "firstName")),
                          PExpr_(3, pj::string_property(res, "firstName")),
                          PExpr_(5, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      query_ctx::start(ctx, {&q1, &q2});
      rs.wait();
      query_ctx::print_plans({&q1, &q2});

      expected.data.push_back({query_result("A"), query_result("D"),
                              query_result("A"), query_result("D")});
      expected.data.push_back({query_result("A"), query_result("C"),
                              query_result("A"), query_result("C")});
      expected.data.push_back({query_result("A"), query_result("B"),
                              query_result("A"), query_result("B")});

      REQUIRE(rs == expected);
    }

    SECTION("join_on_node") {
      result_set rs, expected;
      auto q1 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships(":knows")
                .to_node("Person").get_pipeline();

      auto q2 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships({1, 3}, ":knows")
                .to_node("Person")
                .nested_loop_join({2, 2}, q1)
                .project({PExpr_(0, pj::string_property(res, "firstName")),
                          PExpr_(2, pj::string_property(res, "firstName")),
                          PExpr_(3, pj::string_property(res, "firstName")),
                          PExpr_(5, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      query_ctx::start(ctx, {&q1, &q2});
      rs.wait();
      query_ctx::print_plans({&q1, &q2});

      expected.data.push_back({query_result("A"), query_result("D"),
                              query_result("A"), query_result("D")});
      expected.data.push_back({query_result("A"), query_result("C"),
                              query_result("A"), query_result("C")});
      expected.data.push_back({query_result("A"), query_result("B"),
                              query_result("A"), query_result("B")});

      REQUIRE(rs == expected);
    }

    SECTION("outerjoin on node") {
      std::cout << "outerjoin on node\n";
      result_set rs, expected;
      auto q1 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships(":knows")
                .to_node("Person").get_pipeline();

      auto q2 = query_builder(ctx)
                .all_nodes("Person")
                .property("firstName", [&](auto &p) { return p.equal(graph->get_code("A")); })
                .from_relationships({1, 3}, ":knows")
                .to_node("Person")
                .left_outer_join(q1, [&](const qr_tuple &lv, const qr_tuple &rv) {
                  return boost::get<node *>(lv[2])->id() == boost::get<node *>(rv[2])->id(); })
                .project({PExpr_(0, pj::string_property(res, "firstName")),
                          PExpr_(2, pj::string_property(res, "firstName")),
                          PExpr_(3, pj::string_property(res, "firstName")),
                          PExpr_(5, pj::string_property(res, "firstName"))})
                .collect(rs).get_pipeline();

      query_ctx::start(ctx, {&q1, &q2});
      rs.wait();
      query_ctx::print_plans({&q1, &q2});

      expected.data.push_back({query_result("A"), query_result("D"),
                              query_result("A"), query_result("D")});
      expected.data.push_back({query_result("A"), query_result("C"),
                              query_result("A"), query_result("C")});
      expected.data.push_back({query_result("A"), query_result("B"),
                              query_result("A"), query_result("B")});
      expected.data.push_back({query_result("A"), query_result("E"),
                              query_result("NULL"), query_result("NULL")});
      expected.data.push_back({query_result("A"), query_result("F"),
                              query_result("NULL"), query_result("NULL")});

      REQUIRE(rs == expected);
    }

    SECTION("outerjoin on rship") {
      std::cout << "outerjoin on rship\n";
      result_set rs, expected;
      auto q1 = query_builder(ctx)
                .all_nodes("Person").get_pipeline();

      auto q2 = query_builder(ctx)
                .all_nodes("Person")
                .left_outer_join(q1, [&](const qr_tuple &lv, const qr_tuple &rv) {
                  auto connected = false;
                  auto src = boost::get<node *>(lv[0]);
                  auto des = boost::get<node *>(rv[0]);
                  ctx.foreach_from_relationship_of_node((*src), [&](auto &r) {
                    if (r.to_node_id() == des->id())
                      connected = true;
                  });
                  return connected; })
                .project({PExpr_(0, pj::string_property(res, "firstName")),
                          PExpr_(1, pj::string_property(res, "firstName")) })
                .collect(rs).get_pipeline();

      query_ctx::start(ctx, {&q1, &q2});
      rs.wait();
      query_ctx::print_plans({&q1, &q2});

      expected.data.push_back({query_result("A"), query_result("B")});
      expected.data.push_back({query_result("A"), query_result("C")});
      expected.data.push_back({query_result("A"), query_result("D")});
      expected.data.push_back({query_result("B"), query_result("E")});
      expected.data.push_back({query_result("C"), query_result("NULL")});
      expected.data.push_back({query_result("D"), query_result("NULL")});
      expected.data.push_back({query_result("E"), query_result("F")});
      expected.data.push_back({query_result("F"), query_result("NULL")});

      REQUIRE(rs == expected);
    }

    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing Groupby operator", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph11");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    graph->add_node("Person", {{"age", std::any(42)},
                              {"firstName", std::any(std::string("John"))},
                              {"lastName", std::any(std::string("Doe"))}});

    graph->add_node("Person", {{"age", std::any(77)},
                              {"firstName", std::any(std::string("Michael"))},
                              {"lastName", std::any(std::string("Stonebreaker"))}});

    graph->add_node("Person", {{"age", std::any(48)},
                              {"firstName", std::any(std::string("Anastasia"))},
                              {"lastName", std::any(std::string("Ailamaki"))}});

    graph->add_node("Person", {{"age", std::any(37)},
                              {"firstName", std::any(std::string("John"))},
                              {"lastName", std::any(std::string("Jones"))}});

    graph->add_node("Person", {{"age", std::any(20)},
                              {"firstName", std::any(std::string("John"))},
                              {"lastName", std::any(std::string(""))}});

    graph->add_node("Person", {{"age", std::any(100)},
                              {"firstName", std::any(std::string("Michael"))},
                              {"lastName", std::any(std::string("G."))}});
    return true;
  });
  // TODO: works only if we run queries in a separate transaction!
   ctx.run_transaction([&]() {
   {
      result_set rs, expected;
      auto q = query_builder(ctx)
              .all_nodes("Person")
              .project({PExpr_(0, pj::int_property(res, "age")),
                        PExpr_(0, pj::string_property(res, "firstName")),
                        PExpr_(0, pj::string_property(res, "lastName"))})
              .groupby(
                {
                  // group_by::group{ 0, "firstName", string_type }
                  group_by::group{ 1, "", string_type }
                }, 
                { 
                  // group_by::expr{ group_by::expr::f_count, 0, "age", int_type },
                  group_by::expr{ group_by::expr::f_count, 0, "", int_type },
                  // group_by::expr{ group_by::expr::f_avg, 0, "age", double_type }
                  group_by::expr{ group_by::expr::f_avg, 0, "", double_type },
                  group_by::expr{ group_by::expr::f_sum, 0, "", int_type },
                  group_by::expr{ group_by::expr::f_min, 0, "", int_type },
                  group_by::expr{ group_by::expr::f_max, 0, "", int_type }
                })
//              .groupby({1}, {{"count", 0}, {"pcount", 0}, {"sum", 0},
//                              {"avg", 0}, {"min", 0}, {"max", 0}})
              .collect(rs).get_pipeline();

      q.start(ctx);
      rs.wait();
      q.print_plan();

      expected.data.push_back(
        {query_result("Anastasia"), query_result("1"), query_result("48.000000"),
          query_result("48"), query_result("48"), query_result("48") });
      expected.data.push_back(
        {query_result("Michael"), query_result("2"), query_result("88.500000"),
          query_result("177"),  query_result("77"), query_result("100")});
      expected.data.push_back(
        {query_result("John"), query_result("3"), query_result("33.000000"),
          query_result("99"), query_result("20"), query_result("42")});

      REQUIRE(rs == expected);
    }
    {
      result_set rs, expected;
      auto q = query_builder(ctx)
              .all_nodes("Person")
              .groupby(
                {
                  group_by::group{ 0, "firstName", string_type }
                }, 
                { 
                  group_by::expr{ group_by::expr::f_count, 0, "age", int_type },
                  group_by::expr{ group_by::expr::f_avg, 0, "age", double_type },
                  group_by::expr{ group_by::expr::f_sum, 0, "age", int_type },
                  group_by::expr{ group_by::expr::f_min, 0, "age", int_type },
                  group_by::expr{ group_by::expr::f_max, 0, "age", int_type }
                })
              .collect(rs).get_pipeline();

    q.start(ctx);
    rs.wait();
    q.print_plan();

    expected.data.push_back(
      {query_result("Anastasia"), query_result("1"), query_result("48.000000"),
        query_result("48"), query_result("48"), query_result("48") });
    expected.data.push_back(
      {query_result("Michael"), query_result("2"), query_result("88.500000"),
        query_result("177"),  query_result("77"), query_result("100")});
    expected.data.push_back(
      {query_result("John"), query_result("3"), query_result("33.000000"),
        query_result("99"), query_result("20"), query_result("42")});

    REQUIRE(rs == expected);
    }
    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing Bi-directional traversal operator", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph11");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    auto A = graph->add_node("Person", {{"age", std::any(42)},
                              {"firstName", std::any(std::string("AAA"))},
                              {"lastName", std::any(std::string("aaa"))}});

    auto B = graph->add_node("Person", {{"age", std::any(77)},
                              {"firstName", std::any(std::string("BBB"))},
                              {"lastName", std::any(std::string("bbb"))}});

    auto C = graph->add_node("Person", {{"age", std::any(48)},
                              {"firstName", std::any(std::string("CCC"))},
                              {"lastName", std::any(std::string("ccc"))}});

    auto D = graph->add_node("Person", {{"age", std::any(37)},
                              {"firstName", std::any(std::string("DDD"))},
                              {"lastName", std::any(std::string("ddd"))}});

    auto E = graph->add_node("Person", {{"age", std::any(20)},
                              {"firstName", std::any(std::string("EEE"))},
                              {"lastName", std::any(std::string("eee"))}});
    
    graph->add_relationship(A, C, ":knows", {});
    graph->add_relationship(B, C, ":knows", {});
    graph->add_relationship(C, D, ":knows", {});
    graph->add_relationship(C, E, ":knows", {});

    return true;
  });

  ctx.run_transaction([&]() {

    result_set rs, expected;
    auto q = query_builder(ctx)
              .nodes_where("Person", "age",
                          [&](auto &p) { return p.equal(48); })
              .all_relationships(":knows")
              .project({PExpr_(2, pj::string_property(res, "firstName"))})
              .collect(rs).get_pipeline();

    q.start(ctx);
    rs.wait();
    q.print_plan();

    expected.data.push_back({query_result("EEE")});
    expected.data.push_back({query_result("DDD")});
    expected.data.push_back({query_result("BBB")});
    expected.data.push_back({query_result("AAA")});

    REQUIRE(rs == expected);
    return true;
  });
  graph_pool::destroy(pool);
}

TEST_CASE("Testing distinct operator", "[qop]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qgraph12");
  query_ctx ctx(graph);

  namespace pj = builtin;

  ctx.run_transaction([&]() {

    auto a = graph->add_node("Person", {{"id", std::any(3)}});
    auto b = graph->add_node("Person", {{"id", std::any(4)}});
    auto c = graph->add_node("Person", {{"id", std::any(5)}});
    auto d = graph->add_node("Person", {{"id", std::any(6)}});
    auto p = graph->add_node("Paper", {{"id", std::any(38)}});

    graph->add_relationship(a, p, ":authored", {});
    graph->add_relationship(b, p, ":authored", {});
    graph->add_relationship(c, p, ":authored", {});
    graph->add_relationship(d, p, ":authored", {});

    return true;
  });

  ctx.run_transaction([&]() {

    result_set rs, expected;
    auto q = query_builder(ctx)
              .all_nodes("Person")
              .from_relationships(":authored")
              .to_node("Paper")
              .project({PExpr_(2, pj::int_property(res, "id"))})
              .distinct()
              .collect(rs).get_pipeline();

    q.start(ctx);
    rs.wait();
    q.print_plan();

    expected.data.push_back({query_result("38")});

    REQUIRE(rs == expected);
    return true;
  });
  graph_pool::destroy(pool);
}
