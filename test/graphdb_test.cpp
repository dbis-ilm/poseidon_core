/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
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
#include "graph_db.hpp"

#ifdef USE_PMDK
#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create("/mnt/pmem0/poseidon/graphdb_test", "",
                                    PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
#endif
}

TEST_CASE("Checking adding a node with properties", "[graph_db]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  p_unique_ptr<graph_db> graph;
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
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
  remove("/mnt/pmem0/poseidon/graphdb_test");
#endif
}