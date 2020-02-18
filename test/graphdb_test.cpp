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

#include "catch.hpp"
#include "config.h"
#include "graph_db.hpp"
#include "../qop/qop.hpp"

#ifdef USE_PMDK
#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;
const std::string test_path = poseidon::gPmemPath + "graphdb_test";

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create(test_path, "", PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

TEST_CASE("Creating nodes", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  // TODO
  for (int i = 0; i < 100; i++) {
    auto p1 = graph->add_node("Person",
                              {{"name", boost::any(std::string("John Doe"))},
                               {"age", boost::any(42)},
                               {"number", boost::any(i)},
                               {"dummy1", boost::any(std::string("Dummy"))},
                               {"dummy2", boost::any(1.2345)}},
                              true);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Creating some nodes and relationships", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif
  auto p1 = graph->add_node(":Person", {});
  auto p2 = graph->add_node(":Person", {});
  auto b1 = graph->add_node(":Book", {});
  auto b2 = graph->add_node(":Book", {});
  auto b3 = graph->add_node(":Book", {});

  graph->add_relationship(p1, b1, ":HAS_READ", {});
  graph->add_relationship(p1, b2, ":HAS_READ", {});
  graph->add_relationship(p1, b3, ":HAS_READ", {});
  graph->add_relationship(p2, b3, ":HAS_READ", {});
  graph->add_relationship(p1, p2, ":IS_FRIENDS_WITH", {});

  // check if we have all nodes
  {
    auto &n = graph->node_by_id(p1);
    REQUIRE(n.id() == p1);
    auto n1 = graph->get_node_description(n);
    REQUIRE(n1.label == ":Person");
    REQUIRE(n1.id == p1);

    auto n2 = graph->get_node_description(graph->node_by_id(p2));
    REQUIRE(n2.label == ":Person");
    REQUIRE(n2.id == p2);

    auto n3 = graph->get_node_description(graph->node_by_id(b1));
    REQUIRE(n3.label == ":Book");
    REQUIRE(n3.id == b1);

    auto n4 = graph->get_node_description(graph->node_by_id(b2));
    REQUIRE(n4.label == ":Book");
    REQUIRE(n4.id == b2);

    auto n5 = graph->get_node_description(graph->node_by_id(b3));
    REQUIRE(n5.label == ":Book");
    REQUIRE(n5.id == b3);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking FROM relationships", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif
  auto p1 = graph->add_node(":Person", {});
  auto p2 = graph->add_node(":Person", {});
  auto b1 = graph->add_node(":Book", {});
  auto b2 = graph->add_node(":Book", {});
  auto b3 = graph->add_node(":Book", {});

  graph->add_relationship(p1, b1, ":HAS_READ", {});
  graph->add_relationship(p1, b2, ":HAS_READ", {});
  graph->add_relationship(p1, b3, ":HAS_READ", {});
  graph->add_relationship(p2, b3, ":HAS_READ", {});
  graph->add_relationship(p1, p2, ":IS_FRIENDS_WITH", {});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif
  // check if we have all relationships for each node
  {
    int hasReadCnt = 0;
    int isFriendsCnt = 0;
    auto &n1 = graph->node_by_id(p1);
    graph->foreach_from_relationship_of_node(n1, [&](auto &r) {
      auto s = std::string(graph->get_string(r.rship_label));
      if (s == ":HAS_READ")
        hasReadCnt++;
      else if (s == ":IS_FRIENDS_WITH")
        isFriendsCnt++;
      auto &n2 = graph->node_by_id(r.to_node_id());
      std::cout << n1.id() << "-[" << s << "]->" << n2.id() << std::endl;
    });
    REQUIRE(hasReadCnt == 3);
    REQUIRE(isFriendsCnt == 1);
  }

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking TO relationships", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto p1 = graph->add_node(":Person", {});
  auto p2 = graph->add_node(":Person", {});
  auto b1 = graph->add_node(":Book", {});
  auto b2 = graph->add_node(":Book", {});
  auto b3 = graph->add_node(":Book", {});

  graph->add_relationship(p1, b1, ":HAS_READ", {});
  graph->add_relationship(p1, b2, ":HAS_READ", {});
  graph->add_relationship(p1, b3, ":HAS_READ", {});
  graph->add_relationship(p2, b3, ":HAS_READ", {});
  graph->add_relationship(p1, p2, ":IS_FRIENDS_WITH", {});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif

  {
    int hasReadCnt = 0;
    auto &n1 = graph->node_by_id(b3);
    graph->foreach_to_relationship_of_node(n1, [&](auto &r) {
      auto s = std::string(graph->get_string(r.rship_label));
      if (s == ":HAS_READ")
        hasReadCnt++;
      // auto& n2 = graph->node_by_id(r.from_node_id());
    });
    REQUIRE(hasReadCnt == 2);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking recursive FROM relationships", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  /*
    p1[0]-->p2[1]-->p3[2]-->p4[3]-->p5[4]
    |       |
    |       +-->p7[6]-->p8[7]
    |       |
    |       +->p10[9]
    |       |
    |       +->p9[8]
    |
    +-->p6[5]-->p11[10]
   */
  auto p1 = graph->add_node(":Person", {});
  auto p2 = graph->add_node(":Person", {});
  auto p3 = graph->add_node(":Person", {});
  auto p4 = graph->add_node(":Person", {});
  auto p5 = graph->add_node(":Person", {});
  auto p6 = graph->add_node(":Person", {});
  auto p7 = graph->add_node(":Person", {});
  auto p8 = graph->add_node(":Person", {});
  auto p9 = graph->add_node(":Person", {});
  auto p10 = graph->add_node(":Person", {});
  auto p11 = graph->add_node(":Person", {});

  graph->add_relationship(p1, p2, ":KNOWS", {});
  graph->add_relationship(p2, p3, ":KNOWS", {});
  graph->add_relationship(p3, p4, ":KNOWS", {});
  graph->add_relationship(p4, p5, ":KNOWS", {});
  graph->add_relationship(p2, p7, ":KNOWS", {});
  graph->add_relationship(p7, p8, ":KNOWS", {});
  graph->add_relationship(p2, p10, ":KNOWS", {});
  graph->add_relationship(p2, p9, ":KNOWS", {});
  graph->add_relationship(p1, p6, ":KNOWS", {});
  graph->add_relationship(p6, p11, ":KNOWS", {});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif
  std::set<node::id_t> reachable_nodes;
  auto &n1 = graph->node_by_id(p1);
  graph->foreach_variable_from_relationship_of_node(n1, 1, 3, [&](auto &r) {
    auto &n2 = graph->node_by_id(r.to_node_id());
    reachable_nodes.insert(n2.id());
  });

  REQUIRE(reachable_nodes ==
          std::set<node::id_t>({1, 2, 3, 6, 7, 8, 9, 5, 10}));

  reachable_nodes.clear();
  auto &n2 = graph->node_by_id(p2);
  graph->foreach_variable_from_relationship_of_node(n2, 1, 3, [&](auto &r) {
    auto &n3 = graph->node_by_id(r.to_node_id());
    reachable_nodes.insert(n3.id());
  });

  REQUIRE(reachable_nodes == std::set<node::id_t>({2, 3, 4, 6, 7, 8, 9}));

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking adding a node with properties", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto p1 =
      graph->add_node(":Person", {{"name", boost::any(std::string("John"))},
                                  {"age", boost::any(42)}});

  auto &n = graph->node_by_id(p1);
  REQUIRE(n.id() == p1);

  auto ndescr = graph->get_node_description(n);
  REQUIRE(ndescr.id == p1);
  REQUIRE(ndescr.label == ":Person");
  REQUIRE(ndescr.properties.find("name") != ndescr.properties.end());
  REQUIRE(ndescr.properties.find("age") != ndescr.properties.end());

  REQUIRE(std::string("John") ==
          get_property<const std::string &>(ndescr.properties, "name"));
  REQUIRE(get_property<int>(ndescr.properties, "age") == 42);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking node with properties", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  std::cout << "TX #1" << std::endl;
  auto tx = graph->begin_transaction();
#endif

  auto p1 =
      graph->add_node(":Person", {{"name", boost::any(std::string("John"))},
                                  {"age", boost::any(42)}});

#ifdef USE_TX
  graph->commit_transaction();
  std::cout << "TX #2" << std::endl;
  tx = graph->begin_transaction();
#endif

  auto &n1 = graph->node_by_id(p1);
  auto ndescr = graph->get_node_description(n1);

  std::cout << "---> " << ndescr << std::endl;
  REQUIRE(ndescr.id == p1);
  REQUIRE(ndescr.label == ":Person");
  REQUIRE(ndescr.properties.find("name") != ndescr.properties.end());
  REQUIRE(ndescr.properties.find("age") != ndescr.properties.end());

  REQUIRE(std::string("John") ==
          get_property<const std::string &>(ndescr.properties, "name"));
  REQUIRE(get_property<int>(ndescr.properties, "age") == 42);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking a dirty node with properties", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto p1 =
      graph->add_node(":Person", {{"name", boost::any(std::string("John"))},
                                  {"age", boost::any(42)}});

  auto &n = graph->node_by_id(p1);
  REQUIRE(n.id() == p1);

  auto ndescr = graph->get_node_description(n);
  REQUIRE(ndescr.id == p1);
  REQUIRE(ndescr.label == ":Person");
  REQUIRE(ndescr.properties.find("name") != ndescr.properties.end());
  REQUIRE(ndescr.properties.find("age") != ndescr.properties.end());

  REQUIRE(std::string("John") ==
          get_property<const std::string &>(ndescr.properties, "name"));
  REQUIRE(get_property<int>(ndescr.properties, "age") == 42);
#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking a node update", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif
  node::id_t p1;

#ifdef USE_TX
  {
    // add a new node
    auto tx = graph->begin_transaction();
#endif

    p1 = graph->add_node(":Person",
                         {{"name", boost::any(std::string("John"))},
                          {"age", boost::any(42)},
                          {"dummy1", boost::any(10)},
                          {"dummy2", boost::any(11)},
                          {"dummy3", boost::any(12)},
                          {"city", boost::any(std::string("Berlin"))}});
#ifdef USE_TX
    graph->commit_transaction();
  }
  {
    REQUIRE_THROWS(check_tx_context());
    // perform an update
    auto tx = graph->begin_transaction();
#endif

    auto &n1 = graph->node_by_id(p1);
    graph->update_node(n1, {{"name", boost::any(std::string("Anne"))},
                            {"age", boost::any(43)},
                            {"city", boost::any(std::string("Munich"))},
                            {"zipcode", boost::any(12345)}});
    // and check whether the updates are available within the transaction
    auto &n2 = graph->node_by_id(p1);
    auto ndescr = graph->get_node_description(n2);
    std::cout << "updated node -=-> " << ndescr << std::endl;

    REQUIRE(ndescr.id == p1);
    REQUIRE(ndescr.label == ":Person");

    REQUIRE(std::string("Anne") ==
            get_property<const std::string &>(ndescr.properties, "name"));
    REQUIRE(get_property<int>(ndescr.properties, "age") == 43);
    REQUIRE(std::string("Munich") ==
            get_property<const std::string &>(ndescr.properties, "city"));
    REQUIRE(ndescr.properties.find("zipcode") != ndescr.properties.end());
    REQUIRE(get_property<int>(ndescr.properties, "zipcode") == 12345);
#ifdef USE_TX
    graph->commit_transaction();
  }
  {
    // now we check whether the updates are available also outside the
    // transaction
    REQUIRE_THROWS(check_tx_context());
    auto tx = graph->begin_transaction();

    auto &n2 = graph->node_by_id(p1);
    auto ndescr = graph->get_node_description(n2);

    REQUIRE(ndescr.id == p1);
    REQUIRE(ndescr.label == ":Person");

    REQUIRE(std::string("Anne") ==
            get_property<const std::string &>(ndescr.properties, "name"));
    REQUIRE(get_property<int>(ndescr.properties, "age") == 43);
    REQUIRE(std::string("Munich") ==
            get_property<const std::string &>(ndescr.properties, "city"));
    REQUIRE(ndescr.properties.find("zipcode") != ndescr.properties.end());
    REQUIRE(get_property<int>(ndescr.properties, "zipcode") == 12345);

    graph->commit_transaction();
  }
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking a relationship update", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif
  node::id_t p1;
  relationship::id_t r;
#ifdef USE_TX
  {
    auto tx = graph->begin_transaction();
#endif

    // add two nodes and one relationship
    p1 = graph->add_node("Person", {});
    auto p2 = graph->add_node("Person", {});
    r = graph->add_relationship(
        p1, p2, "KNOWS",
        {{"p1", boost::any(std::string("val"))}, {"p2", boost::any(10)}});

#ifdef USE_TX
    graph->commit_transaction();
  }
  {
    REQUIRE_THROWS(check_tx_context());
    // perform an update
    auto tx = graph->begin_transaction();
#endif

    auto &n1 = graph->node_by_id(p1);
    graph->foreach_from_relationship_of_node(n1, [&](relationship &rel) {
      std::cout << "update relationship: " << graph->get_relationship_label(rel)
                << std::endl;
      graph->update_relationship(rel, {{"p1", boost::any(std::string("val2"))},
                                       {"p2", boost::any(20)},
                                       {"p3", boost::any(30)}});
    });

    // and check whether the updates are available within the transaction
    auto &n2 = graph->node_by_id(p1);
    graph->foreach_from_relationship_of_node(n1, [&](relationship &rel) {
      auto reldesc = graph->get_rship_description(rel);
      std::cout << reldesc << std::endl;

      REQUIRE(reldesc.id == r);
      REQUIRE(reldesc.label == "KNOWS");

      REQUIRE(std::string("val2") ==
              get_property<const std::string &>(reldesc.properties, "p1"));
      REQUIRE(get_property<int>(reldesc.properties, "p2") == 20);
      REQUIRE(get_property<int>(reldesc.properties, "p3") == 30);
    });

#ifdef USE_TX
    graph->commit_transaction();
  }
  {
    // now we check whether the updates are available also outside the
    // transaction
    REQUIRE_THROWS(check_tx_context());
    auto tx = graph->begin_transaction();

    auto &n2 = graph->node_by_id(p1);
    graph->foreach_from_relationship_of_node(n2, [&](relationship &rel) {
      auto reldesc = graph->get_rship_description(rel);
      std::cout << reldesc << std::endl;

      REQUIRE(reldesc.id == r);
      REQUIRE(reldesc.label == "KNOWS");

      REQUIRE(std::string("val2") ==
              get_property<const std::string &>(reldesc.properties, "p1"));
      REQUIRE(get_property<int>(reldesc.properties, "p2") == 20);
      REQUIRE(get_property<int>(reldesc.properties, "p3") == 30);
    });

    graph->commit_transaction();
  }
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Projecting dtimestring property of node", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

auto post_id = graph->add_node(
    "Post",
    {
        {"id", boost::any(13743895)},
        {"creationDate",
          boost::any(builtin::dtimestring_to_int("2011-Oct-05 14:38:36.019"))}});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif

  auto &post = graph->node_by_id(post_id);
  auto post_descr = graph->get_node_description(post);
  auto pr_property = std::string("creationDate");
  auto sec = get_property<int>(post_descr.properties, pr_property);
  assert(std::floor(sec) == sec);
  auto date = builtin::int_to_dtimestring(sec); // this works here -  but it does NOT work in the test below (relationsip)
  //auto test_date = builtin::int_to_dtimestring(1317825516); // this does not work here - but it works in the test below (relationsip)

  REQUIRE(sec == 1317825516);
  REQUIRE(date == "2011-Oct-05 14:38:36");

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

/*TEST_CASE("Projecting dtimestring property of relationship", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto mahinda_id = graph->add_node(
    "Person",
    {{"id", boost::any(933)},
      {"firstName", boost::any(std::string("Mahinda"))},
      {"creationDate",
      boost::any(builtin::dtimestring_to_int("2010-02-14 15:32:10.447"))}});
  auto baruch_id = graph->add_node(
      "Person",
      {{"id", boost::any(4139)},
       {"firstName", boost::any(std::string("Baruch"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-01-28 01:38:17.824"))}}); 
  auto fritz_id = graph->add_node(
      "Person",
      {{"id", boost::any(65970697)},
       {"firstName", boost::any(std::string("Fritz"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-08-24 20:13:46.569"))}});
  auto andrei_id = graph->add_node(
      "Person",
      {{"id", boost::any(10995116)},
       {"firstName", boost::any(std::string("Andrei"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-12-26 14:40:36.649"))}});
  auto ottoR_id = graph->add_node(
      "Person",
      {{"id", boost::any(838375)},
       {"firstName", boost::any(std::string("Otto"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-07-12 03:11:27.663"))}});
  auto ottoB_id = graph->add_node(
      "Person",
      {{"id", boost::any(833579)},
       {"firstName", boost::any(std::string("Otto"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-09-03 07:26:57.953"))}});

  graph->add_relationship(mahinda_id, baruch_id, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2010-03-13 07:37:21.718"))}, 
        {"dummy_property", boost::any(std::string("dummy_1"))}});
  graph->add_relationship(mahinda_id, fritz_id, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2010-09-20 09:42:43.187"))},
        {"dummy_property", boost::any(std::string("dummy_2"))}});
  graph->add_relationship(mahinda_id, andrei_id, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2011-01-02 06:43:41.955"))},
        {"dummy_property", boost::any(std::string("dummy_3"))}});
  graph->add_relationship(mahinda_id, ottoB_id, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))},
        {"dummy_property", boost::any(std::string("dummy_4"))}});
  graph->add_relationship(mahinda_id, ottoR_id, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))},
        {"dummy_property", boost::any(std::string("dummy_5"))}}); // testing date order

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif

  std::set<int> qr_result_sec;
  std::set<std::string> qr_result_date;

  auto &mahinda = graph->node_by_id(mahinda_id);
  graph->foreach_from_relationship_of_node(mahinda, [&](auto &r) {
        auto pr_property = std::string("creationDate");
        auto r_label = std::string(graph->get_string(r.rship_label));
        if (r_label == ":KNOWS"){
          auto &frnd = graph->node_by_id(r.to_node_id());
          auto frnd_label = std::string(graph->get_string(frnd.node_label));
          if (frnd_label == "Person"){
            auto r_descr = graph->get_rship_description(r);
            auto sec = get_property<int>(r_descr.properties, pr_property);
            assert(std::floor(sec) == sec);
            
            auto date = builtin::int_to_dtimestring(sec); 
            it does NOT work here in certain cases (ref issues)
              throws malloc(): memory corruption: 0x0000000002433281
            qr_result_sec.insert(sec);
            qr_result_date.insert(date);

            auto test_date = builtin::int_to_dtimestring(1317825516); // this works here - 
                                                                        // but it does NOT work in the test above (node)

          } 
        }
      });
  
  REQUIRE(qr_result_sec == 
          std::set<int>({1346980290, 1346980290, 1293950621, 1284975763, 1268465841}));

  REQUIRE(qr_result_date == 
          std::set<std::string>({"2012-09-07 01:11:30", "2012-09-07 01:11:30",
                                    "2011-01-02 06:43:41", "2010-09-20 09:42:43",
                                    "2010-03-13 07:37:21"}));
  
  REQUIRE(qr_result_date == 
        std::set<std::string>({"2010-Mar-13 07:37:21", "2010-Sep-20 09:42:43", 
                                "2011-Jan-02 06:43:41", "2012-Sep-07 01:11:30"}));


#ifdef USE_TX
  graph->commit_transaction();
#endif
}*/


TEST_CASE("Projecting only PExpr_ of higher indexes", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  auto hoChi_id = graph->add_node(
      "Person",
      {{"id", boost::any(4194)},
       {"firstName", boost::any(std::string("Hồ Chí"))}, 
       {"lastName", boost::any(std::string("Do"))}});
  auto forum_id = graph->add_node(
      "Forum",
      {{"id", boost::any(37)},
       {"title", boost::any(std::string("Wall of Hồ Chí Do"))}});
  auto post_id = graph->add_node(
      "Post",
      {{"id", boost::any(16492674)}});
  auto comment1_id = graph->add_node(
      "Comment",
      {
          {"id", boost::any(16492675)}});
  auto comment2_id = graph->add_node(
      "Comment",
      {
          {"id", boost::any(16492676)}});
  auto comment3_id = graph->add_node(
      "Comment",
      {
          {"id", boost::any(16492677)}});

  graph->add_relationship(forum_id, post_id, ":containerOf", {});
  graph->add_relationship(forum_id, hoChi_id, ":hasModerator", {});
  graph->add_relationship(comment1_id, post_id, ":replyOf", {});
  graph->add_relationship(comment2_id, comment1_id, ":replyOf", {});
  graph->add_relationship(comment3_id, comment2_id, ":replyOf", {});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif

  std::set<int> qr_result_f_id;
  std::set<int> qr_result_modrt_id;
  std::set<std::string> qr_result_f_title;
  std::set<std::string> qr_result_modrt_fName;
  std::set<std::string> qr_result_modrt_lName;
  
  auto &comment3 = graph->node_by_id(comment3_id);
  graph->foreach_variable_from_relationship_of_node(comment3, 1, 5, [&](auto &r1) {
    auto r1_label = std::string(graph->get_string(r1.rship_label));
    
    if (r1_label == ":replyOf"){
      auto &msg = graph->node_by_id(r1.to_node_id());
      auto msg_label = std::string(graph->get_string(msg.node_label));
      
      if (msg_label == "Post"){
        graph->foreach_to_relationship_of_node(msg, [&](auto &r2) {
          auto r2_label = std::string(graph->get_string(r2.rship_label));
          
          if (r2_label == ":containerOf"){
            auto &forum = graph->node_by_id(r2.from_node_id());
            auto forum_label = std::string(graph->get_string(forum.node_label));
            
            if (forum_label == "Forum"){
              graph->foreach_from_relationship_of_node(forum, [&](auto &r3) {
                auto r3_label = std::string(graph->get_string(r3.rship_label));
                
                if (r3_label == ":hasModerator"){
                  auto &modrt = graph->node_by_id(r3.to_node_id());
                  auto modrt_label = std::string(graph->get_string(modrt.node_label));
                  
                  if (modrt_label == "Person"){
                    auto forum_descr = graph->get_node_description(forum);
                    auto modrt_descr = graph->get_node_description(modrt);
                    auto f_id = get_property<int>(forum_descr.properties, 
                                                  std::string("id"));
                    auto f_title = get_property<std::string>(forum_descr.properties, 
                                                  std::string("title"));
                    auto modrt_id = get_property<int>(modrt_descr.properties, 
                                                  std::string("id"));
                    auto modrt_fName = get_property<std::string>(modrt_descr.properties, 
                                                  std::string("firstName"));
                    auto modrt_lName = get_property<std::string>(modrt_descr.properties, 
                                                  std::string("lastName"));
                    
                    qr_result_f_id.insert(f_id);
                    qr_result_modrt_id.insert(modrt_id);
                    qr_result_f_title.insert(f_title);
                    qr_result_modrt_fName.insert(modrt_fName);
                    qr_result_modrt_lName.insert(modrt_lName);

                  }
                }
              });
            }
          }
        });
      }
    }
  });

  REQUIRE(qr_result_f_id == std::set<int>({37}));
  REQUIRE(qr_result_modrt_id == std::set<int>({4194}));
  REQUIRE(qr_result_f_title == std::set<std::string>({"Wall of Hồ Chí Do"}));
  REQUIRE(qr_result_modrt_fName == std::set<std::string>({"Hồ Chí"}));
  REQUIRE(qr_result_modrt_lName == std::set<std::string>({"Do"}));

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Projecting PExpr_", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

auto lomana_id = graph->add_node(
    "Person",
    {{"id", boost::any(15393)},
      {"firstName", boost::any(std::string("Lomana Trésor"))},
      {"lastName", boost::any(std::string("Kanam"))},
      {"gender", boost::any(std::string("male"))},
      {"birthday", boost::any(builtin::datestring_to_int("1986-09-22"))},
      {"creationDate",
      boost::any(builtin::dtimestring_to_int("2011-04-02 23:53:29.932"))},
      {"locationIP", boost::any(std::string("41.76.137.230"))},
      {"browser", boost::any(std::string("Chrome"))}});
auto amin_id = graph->add_node(
    "Person",
    {{"id", boost::any(19791)},
      {"firstName", boost::any(std::string("Amin"))},
      {"lastName", boost::any(std::string("Kamkar"))},
      {"gender", boost::any(std::string("male"))},
      {"birthday", boost::any(builtin::datestring_to_int("1989-05-24"))},
      {"creationDate",
      boost::any(builtin::dtimestring_to_int("2011-08-30 05:41:09.519"))},
      {"locationIP", boost::any(std::string("81.28.60.168"))},
      {"browser", boost::any(std::string("Internet Explorer"))}});
auto comment1_id = graph->add_node(
    "Comment",
    {
        {"id", boost::any(16492676)},
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 03:24:33.368"))},
        {"locationIP", boost::any(std::string("14.196.249.198"))},
        {"browser", boost::any(std::string("Firefox"))},
        {"content", boost::any(std::string("About Bruce Lee,  sources, in the spirit of "
        "his personal martial arts philosophy, whic"))},
        {"length", boost::any(86)}
    });
auto comment2_id = graph->add_node(
    "Comment",
    {
        {"id", boost::any(1642217)},
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 06:31:18.533"))},
        {"locationIP", boost::any(std::string("41.76.137.230"))},
        {"browser", boost::any(std::string("Chrome"))},
        {"content", boost::any(std::string("maybe"))},
        {"length", boost::any(5)}
    });
auto comment3_id = graph->add_node(
    "Comment",
    {
        {"id", boost::any(16492677)},
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 14:57:10.420"))},
        {"locationIP", boost::any(std::string("81.28.60.168"))},
        {"browser", boost::any(std::string("Internet Explorer"))},
        {"content", boost::any(std::string("I see"))},
        {"length", boost::any(5)}
    });

graph->add_relationship(comment2_id, comment1_id, ":replyOf", {});
graph->add_relationship(comment3_id, comment1_id, ":replyOf", {});
graph->add_relationship(comment2_id, lomana_id, ":hasCreator", {});
graph->add_relationship(comment3_id, amin_id, ":hasCreator", {});

#ifdef USE_TX
  graph->commit_transaction();
  tx = graph->begin_transaction();
#endif

  std::set<int> qr_result_cmnt_id;
  std::set<int> qr_result_author_id;
  std::set<std::string> qr_result_cmnt_content;
  std::set<std::string> qr_result_author_fName;
  std::set<std::string> qr_result_author_lName;

  auto &comment1 = graph->node_by_id(comment1_id);
  graph->foreach_to_relationship_of_node(comment1, [&](auto &r1) {
    auto r1_label = std::string(graph->get_string(r1.rship_label));
    
    if (r1_label == ":replyOf"){
      auto &msg = graph->node_by_id(r1.from_node_id());
      auto msg_label = std::string(graph->get_string(msg.node_label));
      
      if (msg_label == "Comment"){
        graph->foreach_from_relationship_of_node(msg, [&](auto &r2) {
          auto r2_label = std::string(graph->get_string(r2.rship_label));
          
          if (r2_label == ":hasCreator"){
            auto &creator = graph->node_by_id(r2.to_node_id());
            auto creator_label = std::string(graph->get_string(creator.node_label));
            
            if (creator_label == "Person"){
              auto msg_descr = graph->get_node_description(msg);
              auto creator_descr = graph->get_node_description(creator);
              auto cmnt_id = get_property<int>(msg_descr.properties, 
                                            std::string("id"));
              auto cmnt_content = get_property<std::string>(msg_descr.properties, 
                                            std::string("content"));
              auto author_id = get_property<int>(creator_descr.properties, 
                                            std::string("id"));
              auto author_fName = get_property<std::string>(creator_descr.properties, 
                                            std::string("firstName"));
              auto author_lName = get_property<std::string>(creator_descr.properties, 
                                            std::string("lastName"));
              
              qr_result_cmnt_id.insert(cmnt_id);
              qr_result_author_id.insert(author_id);
              qr_result_cmnt_content.insert(cmnt_content);
              qr_result_author_fName.insert(author_fName);
              qr_result_author_lName.insert(author_lName);
            }
          }
        });
      }
    }
  });

  REQUIRE(qr_result_cmnt_id == std::set<int>({1642217, 16492677}));
  REQUIRE(qr_result_author_id == std::set<int>({15393, 19791}));
  REQUIRE(qr_result_cmnt_content == std::set<std::string>({"I see", "maybe"}));
  REQUIRE(qr_result_author_fName == std::set<std::string>({"Amin", "Lomana Trésor"}));
  REQUIRE(qr_result_author_lName == std::set<std::string>({"Kamkar", "Kanam"}));

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
} 
