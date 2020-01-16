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
#include "defs.hpp"
#include "dict.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

struct root {
  nvm::persistent_ptr<dict> dict_p;
};

#endif

TEST_CASE("Inserting some strings", "[dict]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/dict_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop, [&] { root_obj->dict_p = nvm::make_persistent<dict>(); });

  dict &d = *(root_obj->dict_p);
  d.initialize();
#else
  dict d;
#endif

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #4");
  d.insert("String #5");
  d.insert("String #1");
  d.insert("String #2");
  REQUIRE(d.size() == 5);

  std::cout << "end of test" << std::endl;
#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/dict_test");
#endif
}

TEST_CASE("Inserting duplicate strings", "[dict]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/dict_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop, [&] { root_obj->dict_p = nvm::make_persistent<dict>(); });

  dict &d = *(root_obj->dict_p);
  d.initialize();
#else
  dict d;
#endif

  REQUIRE(d.size() == 0);
  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #3");
  d.insert("String #1");
  REQUIRE(d.size() == 3);

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/dict_test");
#endif
}

TEST_CASE("Looking up some strings", "[dict]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/dict_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop, [&] { root_obj->dict_p = nvm::make_persistent<dict>(); });

  dict &d = *(root_obj->dict_p);
  d.initialize();
#else
  dict d;
#endif

  REQUIRE(d.size() == 0);
  auto c1 = d.insert("String #1");
  auto c2 = d.insert("String #2");
  auto c3 = d.insert("String #3");
  auto c4 = d.insert("String #4");
  auto c5 = d.insert("String #5");

  REQUIRE(d.lookup_string("String #4") == c4);
  REQUIRE(d.lookup_string("String #2") == c2);

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/dict_test");
#endif
}

TEST_CASE("Looking up some codes", "[dict]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/dict_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop, [&] { root_obj->dict_p = nvm::make_persistent<dict>(); });

  dict &d = *(root_obj->dict_p);
  d.initialize();
#else
  dict d;
#endif

  REQUIRE(d.size() == 0);
  auto c1 = d.insert("String #1");
  auto c2 = d.insert("String #2");
  auto c3 = d.insert("String #3");
  auto c4 = d.insert("String #4");
  auto c5 = d.insert("String #5");

  REQUIRE(std::string("String #4") == d.lookup_code(c4));
  REQUIRE(std::string("String #3") ==d.lookup_code(c3));

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/dict_test");
#endif
}

TEST_CASE("Looking up some non-existing strings", "[dict]") {
#ifdef USE_PMDK
  auto pop = nvm::pool<root>::create("/mnt/pmem0/poseidon/dict_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  nvm::transaction::run(pop, [&] { root_obj->dict_p = nvm::make_persistent<dict>(); });

  dict &d = *(root_obj->dict_p);
  d.initialize();
#else
  dict d;
#endif

  d.insert("String #1");
  d.insert("String #2");
  d.insert("String #3");
  d.insert("String #4");
  d.insert("String #5");
  REQUIRE(d.lookup_string("Unknown string") == 0);

#ifdef USE_PMDK
  pop.close();
  remove("/mnt/pmem0/poseidon/dict_test");
#endif
}

// TODO
// * test with a large set of strings
// * test persistent dictionary
