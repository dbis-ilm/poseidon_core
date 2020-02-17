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

#include <condition_variable>
#include <iostream>
#include <thread>

#include "catch.hpp"
#include "config.h"
#include "defs.hpp"
#include "graph_db.hpp"
#include "transaction.hpp"

#ifdef USE_PMDK
#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;
const std::string test_path = poseidon::gPmemPath + "transaction_test";

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create(test_path, "", PMEMOBJ_POOL_SIZE);
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
  remove(test_path.c_str());
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
  remove(test_path.c_str());
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
  remove(test_path.c_str());
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
    {
      // wait for thread #1
      std::unique_lock<std::mutex> lock(m);
      cond_var1.wait(lock, [&] { return ready1.load(); });
    }

    auto tx = gdb->begin_transaction(); // wait until thread-1 starts and txid is assigned to it, else race-around condition occurs.

    // update the node
    spdlog::info("thread #2 - node_by_id + update");
    auto &n = gdb->node_by_id(nid);
    gdb->update_node(n, {
    		{"name", boost::any(std::string("Mark Wahlberg updated"))},
		{"age", boost::any(49),}},
    		"Updated Actor");

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
    REQUIRE(nd.label == "Updated Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 49);
    REQUIRE(get_property<std::string>(nd.properties, "name") == "Mark Wahlberg updated");

    gdb->commit_transaction();
    gdb->dump();
  }
#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(gdb); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("Checking that a update transaction is aborted if the object is "
          "already locked",
          "[transaction]") {}

TEST_CASE("Checking the transaction level Garbage Collector basic functionality"
          "updated node",
          "[transaction][Garbage Collector] [gc]") {

#ifdef USE_PMDK
		auto pop = prepare_pool();
		graph_db_ptr gdb;
		nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
		auto gdb = p_make_ptr<graph_db>();
#endif
		std::mutex m;
		std::condition_variable cond_var1, cond_var2, cond_var3, cond_var4;
		std::atomic<bool>  ready1{false}, ready2{false}, ready3{false}, ready4{false};

		node::id_t nid = 0;

		/** Initially, we create a new node.
		 */
		{
			auto tx = gdb->begin_transaction();
			nid = gdb->add_node("Actor",
					{{"name", boost::any(std::string("Mark Wahlberg"))},
							{"salary", boost::any(300)}});
			gdb->commit_transaction();
		}
		/*
		 * Thread #1: start a transaction to write transaction.
		 */
		auto t1 = std::thread([&]() {
			auto tx = gdb->begin_transaction();
			{
				// Notify thread #2, that it can start.
				std::lock_guard<std::mutex> lock(m);
				ready2 = true;
				cond_var2.notify_one();
			}
            // Perform an update
			auto &n = gdb->node_by_id(nid);
			gdb->update_node(n, {
					{"salary", boost::any(300)}});
			gdb->commit_transaction();

			// Dirty list should not exist
			REQUIRE(n.get_dirty_objects().has_value()== false);
			{
				// Notify that thread #1  is committed.
				std::lock_guard<std::mutex> lock(m);
				ready1 = true;
				cond_var1.notify_one();
			}
		});

		/*
		 * Thread #2: Start a transaction to read
		 */
		auto t2 = std::thread([&]() {
			{
				// wait for  Transaction-1  to commit
				std::unique_lock<std::mutex> lock(m);
				cond_var1.wait(lock, [&] { return ready1.load(); });
			}
			auto tx = gdb->begin_transaction();
			{
				// Notify thread #3  to start
				std::lock_guard<std::mutex> lock(m);
				ready3 = true;
				cond_var3.notify_one();
			}
			{   // Read after the updated values, should still read old dirty value.
				auto &n = gdb->node_by_id(nid);
				auto nd = gdb->get_node_description(n);
				REQUIRE(get_property<int>(nd.properties, "salary") == 300);
			}
			{
				// wait for thread#3 to commit
				std::unique_lock<std::mutex> lock(m);
				cond_var4.wait(lock, [&] { return ready4.load(); });
			}
			{
				const auto &n = gdb->node_by_id(nid);
				REQUIRE(n.get_dirty_objects().has_value()== true);

				// There must be one dirty object in dirty list
				REQUIRE( n.get_dirty_objects().value()->size() == 1);
				auto nd = gdb->get_node_description(n.get_dirty_objects().value()->front()->elem_);
				// The dirty object in dirty list is still the same old value.
				REQUIRE(get_property<int>(nd.properties, "salary") == 300);
			}
			gdb->commit_transaction();
		});
		/*
		 * Thread #3: start a transaction to write transaction.
		 */
		auto t3 = std::thread([&]() {
			{
				// wait for t3 start signal
				std::unique_lock<std::mutex> lock(m);
				cond_var3.wait(lock, [&] { return ready3.load(); });
			}
			auto tx = gdb->begin_transaction();
			auto &n = gdb->node_by_id(nid);

			// update
			gdb->update_node(n, {
					{"salary", boost::any(400)}});
			gdb->commit_transaction();

			// Dirty list should still exist after transaction #2 committed
			REQUIRE(n.get_dirty_objects().has_value()== true);
			// There must be one dirty object in dirty list
			REQUIRE( n.get_dirty_objects().value()->size() == 1);
			{
				// inform thread #3  committed
				std::lock_guard<std::mutex> lock(m);
				ready4 = true;
				cond_var4.notify_all();
			}
		});
		/*
		 * Thread #4: start a transaction to read the object.
		 */
		auto t4 = std::thread([&]() {
			{
				// wait for t3 to commit
				std::unique_lock<std::mutex> lock(m);
				cond_var4.wait(lock, [&] { return ready4.load(); });
			}

			//Read transaction
			auto tx = gdb->begin_transaction();
			{
				const auto &n = gdb->node_by_id(nid);
				auto nd = gdb->get_node_description(n);
				// Read the committed value
				REQUIRE(get_property<int>(nd.properties, "salary") == 400);
				// Dirty list should still exist
				REQUIRE(n.get_dirty_objects().has_value()== true);
				// There must be one dirty object in dirty list
				REQUIRE( n.get_dirty_objects().value()->size() == 1);
				//The property: salary on main table should have new updated value 400,
				//while property: salary in dirty list should still contain salary as 300
				const auto& dn_ptr = n.get_dirty_objects().value()->front();
				auto props  = gdb->get_properties()->
						build_properties_from_pitems(dn_ptr->properties_,gdb->get_dictionary());
				auto dn_nd = node_description{dn_ptr->elem_.id(),
					std::string(gdb->get_dictionary()->lookup_code(dn_ptr->elem_.node_label)),
					props};
                 // New salary:400 on main table, old salary:300 on dirty list
				REQUIRE(get_property<int>(nd.properties, "salary") != get_property<int>(dn_nd.properties, "salary"));
			}
			gdb->commit_transaction();
		});

		t1.join(); t2.join(); t3.join(); t4.join();

}

TEST_CASE("Checking the Garbage Collector functionality: Maintain multiple dirty version"
          "updated node",
          "[transaction][Garbage Collector] [gc]") {

#ifdef USE_PMDK
	auto pop = prepare_pool();
	graph_db_ptr gdb;
	nvm::transaction::run(pop, [&] { gdb = p_make_ptr<graph_db>(); });
#else
	auto gdb = p_make_ptr<graph_db>();
#endif
	std::mutex m;
	std::condition_variable cond_var1, cond_var2, cond_var3, cond_var4, cond_var5;
	std::atomic<bool> ready1{false}, ready2{false}, ready3{false}, ready4{false};

	node::id_t nid = 0;

	/**
	 *  Initially, we create a new node.
	 */
	{
		auto tx = gdb->begin_transaction();
		nid = gdb->add_node("Director",
				{{"name", boost::any(std::string("John"))},
						{"salary", boost::any(1000)}});
		gdb->commit_transaction();
	}

	/**
	*  Thread#1  will start a transaction to write
	*/
	auto t1 = std::thread([&]() {
		auto tx = gdb->begin_transaction();
		{
			// Notify thread #2, to start.
			std::lock_guard<std::mutex> lock(m);
			ready2 = true;
			cond_var2.notify_one();
		}
		{
			// wait for transaction#2 to read
			std::unique_lock<std::mutex> lock(m);
			cond_var4.wait(lock, [&] { return ready3.load(); });
		}


		//Then do an update. Ideally, this should result in T2 abort.
		//TODO : Yet to be implemented
		auto &n = gdb->node_by_id(nid);
		gdb->update_node(n, {
				{"salary", boost::any(2000)}});

		gdb->commit_transaction();
	});

	/*
	 * Thread #2: Start a transaction to read.
	 */
	auto t2 = std::thread([&]() {
		auto tx = gdb->begin_transaction();
		{
			auto &n = gdb->node_by_id(nid);
			auto nd = gdb->get_node_description(n);
			// Should read the old value
			REQUIRE(get_property<int>(nd.properties, "salary") == 1000);
		}

		// Notify thread#1 that transaction#2 has read.
		{
			std::lock_guard<std::mutex> lock(m);
			ready3 = true;
			cond_var4.notify_one();
		}
		{
			// wait for transaction-5 to commit
			std::unique_lock<std::mutex> lock(m);
			cond_var5.wait(lock, [&] { return ready4.load(); });
		}
		gdb->commit_transaction();
	});

	t1.join();

	{
		auto tx = gdb->begin_transaction();
		auto &n = gdb->node_by_id(nid);
		// update transaction#3
		gdb->update_node(n, {
				{"salary", boost::any(3000)}});
		gdb->commit_transaction();

		// Since transaction#2 is still active, there must be an object in dirty list
		REQUIRE(n.get_dirty_objects().has_value()== true);
	}

	{
	  auto tx = gdb->begin_transaction();
	  auto &n = gdb->node_by_id(nid);
	  // Update Transaction-4.
	  gdb->update_node(n, {
			  {"salary", boost::any(4000)}});
	  gdb->commit_transaction();

	  // Since transaction#2 is still active, there must be  objects in dirty list. The GC cannot empty the list
	  REQUIRE(n.get_dirty_objects().has_value()== true);
		}

	{
		auto tx = gdb->begin_transaction();
		auto &n = gdb->node_by_id(nid);

		// Update Transaction-5.
		gdb->update_node(n, {
				{"salary", boost::any(5000)}});
		gdb->commit_transaction();

		// Since transaction#2 is still active, the dirty version keeps accumulating
		REQUIRE(n.get_dirty_objects().has_value()== true);
		{
			// Notify transaction #5  committed
			std::lock_guard<std::mutex> lock(m);
			ready4 = true;
			cond_var5.notify_one();
		}
	}

	t2.join();

	// Now a new update, should clear the dirty list
	auto tx = gdb->begin_transaction();
	// Then do an upate
	auto &n = gdb->node_by_id(nid);

	// Update Transaction-6.
	gdb->update_node(n, {
			{"salary", boost::any(6000)}});
	// just before commit, we should see all versions in dirty list
	REQUIRE(n.get_dirty_objects().has_value()== true);
	// The top most element must be the latest updated value
	const auto& dn_ptr = n.get_dirty_objects().value()->front();
	auto props  = gdb->get_properties()->
			build_properties_from_pitems(dn_ptr->properties_,gdb->get_dictionary());
	auto dn_nd = node_description{dn_ptr->elem_.id(),
		std::string(gdb->get_dictionary()->lookup_code(dn_ptr->elem_.node_label)),
		props};
	// check if the top most element must be the latest updated value i.e. 6000
	REQUIRE(get_property<int>(dn_nd.properties, "salary") == 6000);
    // commit update
	gdb->commit_transaction();
	// After commit, the dirty list must be cleared and removed by the Garbage collector
	REQUIRE(n.get_dirty_objects().has_value()== false);

}
