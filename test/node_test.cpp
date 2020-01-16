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
#include "nodes.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

struct root {
  pmem::obj::persistent_ptr<node_list> nlist_p;
};

#endif

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

TEST_CASE("Creating a few nodes in the node list", "[nodes]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/node_list_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop,
                   [&] { root_obj->nlist_p = nvm::make_persistent<node_list>(); });

  node_list &nlist = *(root_obj->nlist_p);
#else
  node_list nlist;
#endif

  auto n1 = nlist.add(node(62));
  auto n2 = nlist.add(node(63));
  auto n3 = nlist.add(node(64));

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  REQUIRE(nlist.get(n2).id() == n2);
  REQUIRE(nlist.get(n2).node_label == 63);

  REQUIRE(nlist.get(n3).id() == n3);
  REQUIRE(nlist.get(n3).node_label == 64);

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/node_list_test");
#endif
}

#ifdef USE_PMDK
TEST_CASE("Creating and restoring a persistent node list", "[nodes]") {
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/node_list_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop,
                   [&] { root_obj->nlist_p = nvm::make_persistent<node_list>(); });

  node_list &nlist = *(root_obj->nlist_p);

  auto n1 = nlist.add(node(62));
  auto n2 = nlist.add(node(63));
  auto n3 = nlist.add(node(64));

  REQUIRE(nlist.get(n1).id() == n1);
  REQUIRE(nlist.get(n1).node_label == 62);

  REQUIRE(nlist.get(n2).id() == n2);
  REQUIRE(nlist.get(n2).node_label == 63);

  REQUIRE(nlist.get(n3).id() == n3);
  REQUIRE(nlist.get(n3).node_label == 64);

  pop.close();

  pop = nvm::pool<root>::open("/mnt/pmem0/poseidon/node_list_test", "");
  root_obj = pop.root();

  node_list &nlist2 = *(root_obj->nlist_p);

  REQUIRE(nlist2.get(n1).id() == n1);
  REQUIRE(nlist2.get(n1).node_label == 62);

  REQUIRE(nlist2.get(n2).id() == n2);
  REQUIRE(nlist2.get(n2).node_label == 63);

  REQUIRE(nlist2.get(n3).id() == n3);
  REQUIRE(nlist2.get(n3).node_label == 64);

  pop.close();

  remove("/mnt/pmem0/poseidon/node_list_test");
}
#endif

TEST_CASE("Deleting a node", "[nodes]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/node_list_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop,
                   [&] { root_obj->nlist_p = nvm::make_persistent<node_list>(); });

  node_list &nlist = *(root_obj->nlist_p);
#else
  node_list nlist;
#endif
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

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/node_list_test");
#endif
}

TEST_CASE("Checking reuse of space in node list", "[nodes]") {
  // TODO
}

TEST_CASE("Checking nodes with properties", "[nodes]") {
  // TODO
}