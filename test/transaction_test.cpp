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

#include <condition_variable>
#include <iostream>
#include <thread>

#include "catch.hpp"
#include "defs.hpp"
#include "graph_db.hpp"
#include "transaction.hpp"

#ifdef USE_PMDK
#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create("/mnt/pmem0/poseidon/transaction_test", "",
                                    PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

TEST_CASE("Checking that a newly inserted record exist in the transaction",
          "[transaction]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr gdb;
  nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
  auto gdb = p_make_ptr<graph_db>();
#endif
  auto tx = gdb->begin_transaction();
  auto nid = gdb->add_node("Movie", {});
  auto &my_node = gdb->node_by_id(nid);
  gdb->commit_transaction();

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(gdb); });
  pop.close();
  remove("/mnt/pmem0/poseidon/transaction_test");
#endif
}

TEST_CASE("Checking that a newly inserted record is not visible in a second "
          "transaction",
          "[transaction]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr gdb;
  nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
  auto gdb = p_make_ptr<graph_db>();
#endif
  std::mutex m;
  std::condition_variable cond_var1, cond_var2;
  node::id_t nid = 0;
  std::atomic<bool> ready1{false}, ready2{false};
  /*
   * Thread #1: create a new node
   */
  auto t1 = std::thread([&]() {
    auto tx = gdb->begin_transaction();
    nid = gdb->add_node("Actor", {});

    {
      // inform thread #2 that we have created a new node
      std::lock_guard<std::mutex> lock(m);
      ready1 = true;
      cond_var1.notify_one();
    }

    {
      // but wait until thread #2 had a chance to try to read it..
      std::unique_lock<std::mutex> lock(m);
      cond_var2.wait(lock, [&] { return ready2.load(); });
    }

    // now we can commit
    gdb->commit_transaction();
  });

  /*
   * Thread #2: try to read this node (should fail)
   */
  auto t2 = std::thread([&]() {
    auto tx = gdb->begin_transaction();
    {
      // wait for thread #1
      std::unique_lock<std::mutex> lock(m);
      cond_var1.wait(lock, [&] { return ready1.load(); });
    }
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    gdb->commit_transaction();

    {
      // inform thread #1 that we are finished
      std::unique_lock<std::mutex> lock(m);
      ready2 = true;
      cond_var2.notify_one();
    }
  });

  t1.join();
  t2.join();

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(gdb); });
  pop.close();
  remove("/mnt/pmem0/poseidon/transaction_test");
#endif
}

TEST_CASE("Checking that a newly inserted record becomes visible after commit",
          "[transaction]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr gdb;
  nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
  auto gdb = p_make_ptr<graph_db>();
#endif
  node::id_t nid = 0;
  {
    auto tx = gdb->begin_transaction();
    nid = gdb->add_node("Movie", {});
    auto &my_node = gdb->node_by_id(nid);
    gdb->commit_transaction();
  }

  REQUIRE_THROWS(check_tx_context());

  {
    auto tx = gdb->begin_transaction();
    auto &my_node = gdb->node_by_id(nid);
    auto descr = gdb->get_node_description(my_node);
    REQUIRE(descr.id == nid);
    REQUIRE(descr.label == "Movie");
    gdb->commit_transaction();
  }

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(gdb); });
  pop.close();
  remove("/mnt/pmem0/poseidon/transaction_test");
#endif
}

TEST_CASE("Checking that a read transaction reads the correct version of a "
          "updated node",
          "[transaction]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr gdb;
  nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
  auto gdb = p_make_ptr<graph_db>();
#endif
  std::mutex m;
  std::condition_variable cond_var1, cond_var2;
  node::id_t nid = 0;
  std::atomic<bool> ready1{false}, ready2{false};

  /**
   *  First, we create a new node.
   */
  {
    auto tx = gdb->begin_transaction();
    nid = gdb->add_node("Actor",
                        {{"name", boost::any(std::string("Mark Wahlberg"))},
                         {"age", boost::any(48)}});
    gdb->commit_transaction();
  }
  /*
   * Thread #1: start a transaction to read the object but wait for the other
   * transaction.
   */
  auto t1 = std::thread([&]() {
    auto tx = gdb->begin_transaction();

    {
      // inform thread #2 that we have started the transaction
      std::lock_guard<std::mutex> lock(m);
      ready1 = true;
      cond_var1.notify_one();
    }

    {
      // wait until thread #2 has committed.
      std::unique_lock<std::mutex> lock(m);
      cond_var2.wait(lock, [&] { return ready2.load(); });
    }

    spdlog::info("thread #1 - node_by_id");
    auto &n = gdb->node_by_id(nid);
    auto nd = gdb->get_node_description(n);
    REQUIRE(nd.label == "Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 48);

    gdb->commit_transaction();
  });

  /*
   * Thread #2: for updating
   */
  auto t2 = std::thread([&]() {
    auto tx = gdb->begin_transaction();
    {
      // wait for thread #1
      std::unique_lock<std::mutex> lock(m);
      cond_var1.wait(lock, [&] { return ready1.load(); });
    }

    // update the node
    spdlog::info("thread #2 - node_by_id + update");
    auto &n = gdb->node_by_id(nid);
    gdb->update_node(n, {{"age", boost::any(49)}});

    // and commit
    gdb->commit_transaction();

    {
      // inform thread #1 that we are finished
      std::unique_lock<std::mutex> lock(m);
      ready2 = true;
      cond_var2.notify_one();
    }
  });

  t1.join();
  t2.join();

  {
    auto tx = gdb->begin_transaction();
    spdlog::info("thread #3 - node_by_id");
    auto &n = gdb->node_by_id(nid);
    auto nd = gdb->get_node_description(n);
    REQUIRE(nd.label == "Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 49);

    gdb->commit_transaction();
    gdb->dump();
  }
#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(gdb); });
  pop.close();
  remove("/mnt/pmem0/poseidon/transaction_test");
#endif
}

TEST_CASE("Checking that a update transaction is aborted if the object is "
          "already locked",
          "[transaction]") {}