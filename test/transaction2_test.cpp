/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#include <condition_variable>
#include <iostream>
#include <thread>

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "defs.hpp"
#include "graph_db.hpp"
#include "transaction.hpp"
#include "graph_pool.hpp"

const std::string test_path = PMDK_PATH("transaction2_tst");

TEST_CASE("Test node update followed by delete"  "[transaction]") {  
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph30");

  node::id_t nid;
  gdb->run_transaction([&]() {
    nid = gdb->add_node("Actor",
 		                {{"name", std::any(std::string("Mark Wahlberg"))},
 		                  {"age", std::any(48)}});
    return true;
  });

  gdb->run_transaction([&]() {
    auto &n = gdb->node_by_id(nid);
    gdb->update_node(n, {{"name", std::any(std::string("Mark Wahlberg"))},
 		                  {"age", std::any(49)}}, "Actor");
    gdb->detach_delete_node(nid);
    return true;
  });

  gdb->run_transaction([&]() {
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    return true;
  });

  graph_pool::destroy(pool);
}

TEST_CASE("Test relationship update followed by delete"  "[transaction]") {  
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph31");

  relationship::id_t rid;
  gdb->run_transaction([&]() {
    node::id_t a = gdb->add_node("Actor",
 		                {{"name", std::any(std::string("Mark"))},
 		                  {"age", std::any(48)}});
    node::id_t b = gdb->add_node("Actor",
 		                {{"name", std::any(std::string("Wahlberg"))},
 		                  {"age", std::any(70)}});
    rid = gdb->add_relationship(a, b, ":costarred", {});
    return true;
  });

  gdb->run_transaction([&]() {
    auto &r = gdb->rship_by_id(rid);
    gdb->update_relationship(r, {}, ":costarred");
    gdb->delete_relationship(rid);
    return true;
  });

  graph_pool::destroy(pool); 
}