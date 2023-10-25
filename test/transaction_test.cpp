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

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "defs.hpp"
#include "graph_db.hpp"
#include "query_ctx.hpp"
#include "transaction.hpp"
#include "graph_pool.hpp"

/**
 * A helper class implementing a barrier allowing two threads to coordinate
 * execution.
 */
class barrier {
private:
  std::mutex m;
  std::condition_variable cond_var;
  std::atomic<bool> ready{false};

public:
  barrier() = default;

	/**
	 * Inform the other thread that it can proceed.
	 */
  void notify() {
    std::lock_guard<std::mutex> lock(m);
    ready = true;
    cond_var.notify_one();
  }

	/**
	 * Wait on the barrier until the other threads calls notify.
	 */
  void wait() {
    std::unique_lock<std::mutex> lock(m);
    cond_var.wait(lock, [&] { return ready.load(); });
  }
};

const std::string test_path = PMDK_PATH("transaction_tst");

TEST_CASE("Test transaction execution"  "[transaction]") {  
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph1");

  gdb->run_transaction([&]() {
    gdb->add_node("Actor",
		                {{"name", std::any(std::string("Mark Wahlberg"))},
		                  {"age", std::any(48)}});
    return true;
  });

  graph_pool::destroy(pool);
}

TEST_CASE("Test concurrency: update during read"  "[transaction]") {  
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph2");

	  node::id_t nid = 0;
	  barrier  b1{}, b2{}, b3{};

	  // Just create a node
		gdb->begin_transaction();
		nid = gdb->add_node("Actor",
		                        {{"name", std::any(std::string("Mark Wahlberg"))},
		                         {"age", std::any(48)}});
		gdb->commit_transaction();

	/*
	 * Thread #1: read the node description
	 */
	  auto t1 = std::thread([&]() {
			// start the transaction
			gdb->begin_transaction();

      // inform tx #2 that we started already
	    b1.notify();  
      // but wait until tx #2 has updated a node
      b2.wait();
	    auto nd = gdb->get_node_description(nid);

      // Since this read txn started before the update txn, 
      // we should always read the original version.
			REQUIRE(nd.label == "Actor"); 
			REQUIRE(get_property<int>(nd.properties, "age") == 48); 

			gdb->commit_transaction();
    });

	/*
	 * Thread #2: update the same node
	 */
	  auto t2 = std::thread([&]() {
			b1.wait(); // ensure that update txn starts after read txn
			gdb->begin_transaction();
			auto &n = gdb->node_by_id(nid);
			gdb->update_node(n,  //update
				   {
				 { "age", std::any(52)},
				  },
				   "Updated Actor");

			b2.notify();
			gdb->commit_transaction();
	});

	  t1.join();
	  t2.join();

  graph_pool::destroy(pool);
}

TEST_CASE("Test concurrency: update + commit during read"  "[transaction]") {  
	/*
	* If two concurrent write Transactions are triggered, then the end result must be that there are two seperate
	* nodes created
	*/
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph3");

	  node::id_t nid = 0;
	  barrier  b1{}, b2{}, b3{};

	  // Just create a node
		gdb->begin_transaction();
		nid = gdb->add_node("Actor",
		                        {{"name", std::any(std::string("Mark Wahlberg"))},
		                         {"age", std::any(48)}});
		gdb->commit_transaction();

	/*
	 * Thread #1: read the node
	 */
	  auto t1 = std::thread([&]() {
			// read the node
			gdb->begin_transaction();

	    b1.notify();  // so that txn-3 can start.
	    b2.wait(); // wait till txn-3 does a update but not yet committed

	    auto nd = gdb->get_node_description(nid);
      // Since the Read Txn started before Update Txn, it should always read the original version.
			REQUIRE(nd.label == "Actor"); // It fails here because this txn sees updated Actor
			REQUIRE(get_property<int>(nd.properties, "age") == 48); //here too.. it sees 52 instead of 48

			gdb->commit_transaction();
    });

	/*
	 * Thread #2: update the same node
	 */
	  auto t2 = std::thread([&]() {
			// update the node
			b1.wait(); // ensure that update Txn starts after read Txn
			gdb->begin_transaction();
			auto &n = gdb->node_by_id(nid);
			gdb->update_node(n,  //update
				   {
				 { "age", std::any(52)},
				  },
				   "Updated Actor");
			gdb->commit_transaction();
			b2.notify();
	});

	  t1.join();
	  t2.join();

  graph_pool::destroy(pool);
}

/* Test case for issue: https://dbgit.prakinf.tu-ilmenau.de/code/poseidon_core/-/issues/27
 * Where is the issue ? : It crashes when Read Txn has entered this else block at line https://dbgit.prakinf.tu-ilmenau.de/code/poseidon_core/-/blob/master/src/storage/graph_db.cpp#L580
 *                              while the update Txn deletes the dirty version. 
 * Probable fix : Retain the dirty version until a older Read Txn commits. But now there are 2 identical versions: one on Pmem and one in dirty list.
 *
 */

TEST_CASE("Test concurrency between update abort and read"  "[transaction]") { 
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph4");

  node::id_t nid = 0;
  barrier  b1{}, b2{}, b3{};

  // Just create a node
	gdb->begin_transaction(); 
	nid = gdb->add_node("Actor",
	                        {{"name", std::any(std::string("Mark Wahlberg"))},
	                         {"age", std::any(48)}});
  gdb->commit_transaction();

 /*
  * Thread #1: read the node
  */
  auto t1 = std::thread([&]() { 
		// read the node
		gdb->begin_transaction();
		b1.notify(); // Inform thread #2 to start
		b2.wait(); // wait until thread #2 has performed the update
		
		auto nd = gdb->get_node_description(nid); 
		b3.notify(); // inform thread #2 that we have read the node 

		nd = gdb->get_node_description(nid); 

		REQUIRE(nd.label == "Actor"); 
		REQUIRE(get_property<int>(nd.properties, "age") == 48); 
		gdb->commit_transaction();
  });

  /*
   * Thread #2: update the same node
  */
  auto t2 = std::thread([&]() { 
		// update the node
		b1.wait(); // ensure that update starts after the read transaction (thread #1)
		gdb->begin_transaction();
		auto &n = gdb->node_by_id(nid);
		gdb->update_node(n,  
			   {{ "age", std::any(52)}}, "Updated Actor");

		b2.notify(); // Inform txn-2 that update is done but not yet committed or aborted
	  b3.wait(); // wait until Txn-2 has accessed a dirty version

		gdb->abort_transaction();  // abort the update
  });

  t1.join();
  t2.join();

  graph_pool::destroy(pool);
}

TEST_CASE("Test two concurrent transactions trying to create nodes"  "[transaction]") {
/*
* If two concurrent write Transactions are triggered, then the end result must be that there are two seperate
* nodes created
*/
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph5");

  node::id_t nid1 = 0, nid2 = 0;

  /*
   * Thread #1: Add node
   */

  auto t1 = std::thread([&]() {
	    gdb->begin_transaction();
	    nid1 = gdb->add_node("New Actor",
	                        {{"name", std::any(std::string("Mark Wahlberg"))},
	                         {"age", std::any(48)}});
	                         
	    gdb->commit_transaction();

  });

  /*
   * Thread #2: concurrently add node
   */

	  auto t2 = std::thread([&]() {
		    gdb->begin_transaction();
		    nid2 = gdb->add_node("Actor",
		                        {{"name", std::any(std::string("Peter"))},
		                         {"age", std::any(22)}});
		    gdb->commit_transaction();

 });
  
//---------------------------------------------------------------

  t1.join();
  t2.join();
  
  //verify that two seperate nodes are created.
    {
    // check the node
    gdb->begin_transaction();
    auto nd1 = gdb->get_node_description(nid1);
    std::cout << nd1 << std::endl;
    REQUIRE(nd1.label == "New Actor");
    REQUIRE(get_property<int>(nd1.properties, "age") == 48);
    
    auto nd2 = gdb->get_node_description(nid2);
    REQUIRE(nd2.label == "Actor");
    REQUIRE(get_property<int>(nd2.properties, "age") == 22);  
    gdb->commit_transaction();
  }
  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted node exist in the transaction",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph6");

  gdb->begin_transaction();
  auto nid = gdb->add_node("Movie", {});
  auto &my_node = gdb->node_by_id(nid);
  REQUIRE(my_node.id() == nid);
  gdb->commit_transaction();

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted relationship exist in the transaction",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph7");

  gdb->begin_transaction();
  auto m = gdb->add_node("Movie", {});
  auto a = gdb->add_node("Actor", {});
  auto rid = gdb->add_relationship(a, m, ":PLAYED_IN", {});
 
  auto &my_rship = gdb->rship_by_id(rid);
  REQUIRE(my_rship.id() == rid);
  gdb->commit_transaction();

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a node update is undone after abort", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph8");

  node::id_t nid = 0;
  {
    // create node
    gdb->begin_transaction();
    nid = gdb->add_node("Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});
    gdb->commit_transaction();
  }

  {
    // update node
    gdb->begin_transaction();
    auto &n = gdb->node_by_id(nid);
    gdb->update_node(n,
                     {{
                         "age",
                         std::any(52),
                     }},
                     "Updated Actor");
    // but abort
    gdb->abort_transaction();
  }

  {
    // check the node
    gdb->begin_transaction();
    // auto &n = gdb->node_by_id(nid);
    auto nd = gdb->get_node_description(nid);
    REQUIRE(nd.label == "Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 48);
    gdb->abort_transaction();
  }

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a relationship update is undone after abort", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph9");

  relationship::id_t rid = 0;
  {
    // create node
    gdb->begin_transaction();
    auto m = gdb->add_node("Movie", {});
    auto a = gdb->add_node("Actor", {});
    rid = gdb->add_relationship(a, m, ":PLAYED_IN", {{"role", std::any(std::string("Killer"))}});

    gdb->commit_transaction();
  }

  {
    // update relationship
    gdb->begin_transaction();
    auto &r = gdb->rship_by_id(rid);
    gdb->update_relationship(r,
                     {{
                         "role",
                         std::any(std::string("Cop")),
                     }},
                     ":PLAYED_AS");
    // but abort
    gdb->abort_transaction();
  }

  {
    // check the node
    gdb->begin_transaction();
    // auto &r = gdb->rship_by_id(rid);
    auto rd = gdb->get_rship_description(rid);
    REQUIRE(rd.label == ":PLAYED_IN");
    REQUIRE(get_property<std::string>(rd.properties, "role") == "Killer");
    gdb->abort_transaction();
  }

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a node insert is undone after abort", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph10");

  node::id_t nid = 0;
  {
    gdb->begin_transaction();
    nid = gdb->add_node("Movie", {});
    gdb->abort_transaction();
  }

  {
    gdb->begin_transaction();
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    gdb->abort_transaction();
  }

  graph_pool::destroy(pool);
}


/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a relationship insert is undone after abort", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph11");
  query_ctx ctx(gdb);
  node::id_t m, a;
  relationship::id_t rid;

  ctx.run_transaction([&]() {
    m = gdb->add_node("Movie", {});
    a = gdb->add_node("Actor", {});
    return true;
  });

  ctx.run_transaction([&]() {
    rid = gdb->add_relationship(a, m, ":PLAYED_IN", {{"role", std::any(std::string("Killer"))}});
    return false;
  });

  ctx.run_transaction([&]() {
    // try to access via rid

    REQUIRE_THROWS_AS(gdb->rship_by_id(rid), unknown_id);

    // try to access via node
    auto& n = gdb->node_by_id(a);
    
    bool found = false;
    ctx.foreach_from_relationship_of_node(n, [&found](auto& rship) {
      found = true;
    });
    
    REQUIRE(!found);
    ctx.foreach_to_relationship_of_node(n, [&found](auto& rship) {
      found = true;
    });
    REQUIRE(!found);
    return false;
  });

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted node is not visible in another "
          "transaction",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph12");

  node::id_t nid = 0;
  barrier b1, b2;
  /*
   * Thread #1: create a new node
   */
  auto t1 = std::thread([&]() {
    gdb->begin_transaction();
    nid = gdb->add_node("Actor", {});

    // inform thread #2 that we have created a new node
    b1.notify();
    // but wait until thread #2 had a chance to try to read it..
    b2.wait();

    // now we can commit
    gdb->commit_transaction();
  });

  /*
   * Thread #2: try to read this node (should fail)
   */
  auto t2 = std::thread([&]() {
    // wait for thread #1
    b1.wait();
    gdb->begin_transaction();
    // std::cout << "-------------------1---------------\n";
    // gdb->dump();
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    gdb->commit_transaction();

    // inform thread #1 that we are finished
    b2.notify();
  });

  t1.join();
  t2.join();

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted relationship is not visible in another "
          "transaction",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph13");

  node::id_t m, a;
  {
    gdb->begin_transaction();
    m = gdb->add_node("Movie", {});
    a = gdb->add_node("Actor", {});
    gdb->commit_transaction();
  }

  barrier b1, b2;
  relationship::id_t rid;

  /*
   * Thread #1: create a new relationship
   */
  auto t1 = std::thread([&]() {
    gdb->begin_transaction();
    rid = gdb->add_relationship(a, m, ":PLAYED_IN", {{"role", std::any(std::string("Killer"))}});

    // inform thread #2 that we have created a new node
    b1.notify();
    // but wait until thread #2 had a chance to try to read it..
    b2.wait();

    // now we can commit
    gdb->commit_transaction();
  });

  /*
   * Thread #2: try to read this relationship (should fail)
   */
  auto t2 = std::thread([&]() {
    // wait for thread #1
    b1.wait();
    gdb->begin_transaction();
    // std::cout << "-------------------1---------------\n";
    // gdb->dump();
    REQUIRE_THROWS_AS(gdb->rship_by_id(rid), unknown_id);
    gdb->commit_transaction();

    // inform thread #1 that we are finished
    b2.notify();
  });

  t1.join();
  t2.join();

  graph_pool::destroy(pool);
}
/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted node becomes visible after commit",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph14");

  node::id_t nid = 0;
  {
    gdb->begin_transaction();
    nid = gdb->add_node("Movie", {});
    auto &my_node = gdb->node_by_id(nid);
    REQUIRE(my_node.id() == nid);
    gdb->commit_transaction();
  }

  REQUIRE_THROWS(check_tx_context());

  {
    gdb->begin_transaction();
    auto descr = gdb->get_node_description(nid);
    REQUIRE(descr.id == nid);
    REQUIRE(descr.label == "Movie");
    gdb->commit_transaction();
  }

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a newly inserted relationship becomes visible after commit",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph15");

  node::id_t m, a;
  {
    gdb->begin_transaction();
    m = gdb->add_node("Movie", {});
    a = gdb->add_node("Actor", {});
    gdb->commit_transaction();
  }

  relationship::id_t rid = 0;
  {
    gdb->begin_transaction();
    rid = gdb->add_relationship(a, m, ":PLAYED_IN", {{"role", std::any(std::string("Killer"))}});
    gdb->commit_transaction();
  }

  REQUIRE_THROWS(check_tx_context());

  {
    gdb->begin_transaction();
    auto descr = gdb->get_rship_description(rid);
    REQUIRE(descr.id == rid);
    REQUIRE(descr.label == ":PLAYED_IN");
    gdb->commit_transaction();
  }

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */
TEST_CASE("Checking that a read transaction reads the correct version of a "
          "updated node",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph16");

  node::id_t nid = 0;
  barrier b1, b2;

  /**
   *  First, we create a new node.
   */
  {
    gdb->begin_transaction();
    nid = gdb->add_node("Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});
    gdb->commit_transaction();
  }
  /*
   * Thread #1: start a transaction to read the object but wait for the other
   * transaction.
   */
  auto t1 = std::thread([&]() {
    gdb->begin_transaction();

    // inform thread #2 that we have started the transaction
    b1.notify();

    // wait until thread #2 has committed.
    b2.wait();

    // spdlog::info("read node @{}", (unsigned long)&n);
    auto nd = gdb->get_node_description(nid);
    REQUIRE(nd.label == "Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 48);

    gdb->commit_transaction();
  });

  /*
   * Thread #2: for updating
   */
  auto t2 = std::thread([&]() {
    // wait for thread #1
    b1.wait();

    gdb->begin_transaction(); // wait until thread #1 starts

    // update the node
    auto &n = gdb->node_by_id(nid);
    gdb->update_node(
        n,
        {{"name", std::any(std::string("Mark Wahlberg updated"))},
         {
             "age",
             std::any(49),
         }},
        "Updated Actor");

    // and commit
    gdb->commit_transaction();

    // inform thread #1 that we are finished
    b2.notify();
  });

  t1.join();
  t2.join();

  {
    gdb->begin_transaction();
    auto nd = gdb->get_node_description(nid);
    REQUIRE(nd.label == "Updated Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 49);
    REQUIRE(get_property<std::string>(nd.properties, "name") ==
            "Mark Wahlberg updated");

    gdb->commit_transaction();
    // gdb->dump();
  }
  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a update transaction is aborted if the object is "
          "already locked by another transaction",
          "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph17");

barrier b1, b2, b3;
  // 1. create a new node
  node::id_t nid = 0;
	{
		gdb->begin_transaction();
		// spdlog::info("BOT #1: {}", short_ts(tx->xid()));
    nid = gdb->add_node("Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});
   //  spdlog::info("updated #1");
		gdb->commit_transaction();
	}
  // 2. start a transaction to update
    auto t1 = std::thread([&]() {
		b1.wait();
		gdb->begin_transaction();
		// spdlog::info("BOT #2: {}", short_ts(tx->xid()));
		// perform update
    // spdlog::info("node_by_id #2");
   		auto &n = gdb->node_by_id(nid);
    // spdlog::info("update_node #2");
    	gdb->update_node(n,
                     {{
                         "age",
                         std::any(52),
                     }},
                     "Updated Actor");
		b2.notify();
		// but don't commit yet - let's make sure that the other transaction tries 
		// to update, too
		b3.wait();
		gdb->commit_transaction();
	});
  // 3. start a concurrent transaction which tries to read the same node
    auto t2 = std::thread([&]() {
		b1.notify();
		gdb->begin_transaction();
		b2.wait();

   		auto &n = gdb->node_by_id(nid);
		// try to update: should fail
    	REQUIRE_THROWS_AS(gdb->update_node(n,
                     {{
                         "age",
                         std::any(55),
                     }}), transaction_abort);
		b3.notify();
		gdb->abort_transaction();
	});

	t1.join();
	t2.join();

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking basic transaction level GC", "[transaction][gc]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph18");

  node::id_t nid = 0;
	{
  // create a new node
		gdb->begin_transaction();
    	nid = gdb->add_node("Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});

		gdb->commit_transaction();
	}
	{
		// try to read it - the dirty list should be empty
		gdb->begin_transaction();
		auto &n = gdb->node_by_id(nid);
		REQUIRE(n.get_dirty_objects().has_value() == false);
		gdb->abort_transaction();
	}

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking GC for concurrent transactions", "[transaction][gc]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph19");

	barrier b1, b2, b3, b4;
  node::id_t nid = 0;
	{
  // create a new node
		gdb->begin_transaction();
    	nid = gdb->add_node("Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});

		gdb->commit_transaction();
	}

		
	auto t1 = std::thread([&]() {
    // wait for start of tx #2 to ensure that t1 is newer
    b1.wait();
		gdb->begin_transaction();
		// spdlog::info("BOT #1: {}", short_ts(tx->xid()));

		// perform update
    // spdlog::info("read #1");
		auto &n = gdb->node_by_id(nid);
    // spdlog::info("update #1");
		gdb->update_node(n,
                     {{
                         "age",
                         std::any(55),
                     }}, "Updated Actor");
    // inform tx #2 that we have updated the object
		b2.notify();
		// gdb->dump();
    b3.wait();
		gdb->commit_transaction();
		// spdlog::info("COMMIT #1");

		REQUIRE(n.get_dirty_objects().has_value());
	  b4.notify();
	});

	auto t2 = std::thread([&]() {
		gdb->begin_transaction();
		// spdlog::info("BOT #2 {}", short_ts(tx->xid()));

    b1.notify();
		// read
    // spdlog::info("read #2.1");
    // gdb->dump();
   	auto nd = gdb->get_node_description(nid);
    REQUIRE(nd.label == "Actor");
    REQUIRE(get_property<int>(nd.properties, "age") == 48);
    // spdlog::info("read #2.1 done {}", n.is_dirty());
 
    b2.wait();
		// make sure we still can read our version after the update of tx #1
    // spdlog::info("read #2.2 {}", n.is_dirty());
   	auto nd2 = gdb->get_node_description(nid);
    REQUIRE(nd2.label == "Actor");
    REQUIRE(get_property<int>(nd2.properties, "age") == 48);
    b3.notify();

    b4.wait();
		// make sure we still can read our version after the commit of tx #1
    // spdlog::info("read #2.3 {}", n.is_dirty());
    // gdb->dump();
   	auto nd3 = gdb->get_node_description(nid);
    REQUIRE(get_property<int>(nd3.properties, "age") == 48);
    REQUIRE(nd3.label == "Actor");

		gdb->commit_transaction();
		// spdlog::info("COMMIT #2");
	});

	t1.join();
	t2.join();

	{
		gdb->begin_transaction();
		auto &n = gdb->node_by_id(nid);

		// the dirty_list should still be empty now
    if (n.get_dirty_objects().has_value())
      gdb->dump();
		// REQUIRE(n.get_dirty_objects().has_value() == false);
		gdb->abort_transaction();

	}
  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that deleting a node works", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph20");

  node::id_t nid;
  {
    // add a few nodes
    gdb->begin_transaction();
      gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
     nid = gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
      gdb->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                  {"age", std::any(58)}});

    gdb->commit_transaction();
  }
  {
    // delete the node
    gdb->begin_transaction();
    gdb->delete_node(nid);
    gdb->commit_transaction();
  }

  {
    // check that the node doesn't exist anymore
    gdb->begin_transaction();
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    gdb->abort_transaction();
  }
   graph_pool::destroy(pool); 
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that deleting a node works also within a transaction", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph21");

  node::id_t nid;
  {
    // add a few nodes
    gdb->begin_transaction();
    gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
    nid =
      gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
    gdb->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                  {"age", std::any(58)}});

    gdb->commit_transaction();
  }
  {
    // delete the node
    gdb->begin_transaction();
    gdb->delete_node(nid);
    REQUIRE_THROWS_AS(gdb->node_by_id(nid), unknown_id);
    gdb->commit_transaction();
  }

  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that detach delete a node works", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph22");

  node::id_t a, b, c, d, e;
  {
    // add a few nodes
    gdb->begin_transaction();
      a = gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
      b = gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
      c = gdb->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                  {"age", std::any(58)}});
      d = gdb->add_relationship(a, b, ":knows", {});
      e = gdb->add_relationship(a, c, ":knows", {});

    gdb->commit_transaction();
  }
  {
    // delete the node and all its relationships
    gdb->begin_transaction();
    gdb->detach_delete_node(a);
    gdb->commit_transaction();
  }

  {
    // check that the node doesn't exist anymore
    gdb->begin_transaction();
    REQUIRE_THROWS_AS(gdb->node_by_id(a), unknown_id);
    REQUIRE_THROWS_AS(gdb->rship_by_id(d), unknown_id);
    REQUIRE_THROWS_AS(gdb->rship_by_id(e), unknown_id);
    gdb->abort_transaction();
  }
  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that detach delete also works within a transaction", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph23");

  node::id_t a, b, c, d, e;
  {
    // add a few nodes
    gdb->begin_transaction();
      a = gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
      b = gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
      c = gdb->add_node(":Person", {{"name", std::any(std::string("Pete"))},
                                  {"age", std::any(58)}});
      d = gdb->add_relationship(a, b, ":knows", {});
      e = gdb->add_relationship(a, c, ":knows", {});

    gdb->commit_transaction();
  }
  {
    // delete the node
    gdb->begin_transaction();
    gdb->detach_delete_node(a);
    REQUIRE_THROWS_AS(gdb->node_by_id(a), unknown_id);
    REQUIRE_THROWS_AS(gdb->rship_by_id(d), unknown_id);
    REQUIRE_THROWS_AS(gdb->rship_by_id(e), unknown_id);
    gdb->abort_transaction();
  }
   graph_pool::destroy(pool); 
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that aborting a delete transaction works", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph24");

  node::id_t nid;
  {
    gdb->begin_transaction();
    gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
    nid =
      gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
    gdb->commit_transaction();
  }
  {
    gdb->begin_transaction();
    gdb->delete_node(nid);
    gdb->abort_transaction();
  }
  {
    gdb->begin_transaction();
    REQUIRE_NOTHROW(gdb->node_by_id(nid));
    gdb->abort_transaction();
  }
  
  graph_pool::destroy(pool);
}

/* ----------------------------------------------------------------------- */

TEST_CASE("Checking that a delete transaction does not interfer with another transaction", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph25");

  barrier b1, b2, b3;
  node::id_t nid;

 {
    gdb->begin_transaction();
    gdb->add_node(":Person", {{"name", std::any(std::string("John"))},
                                  {"age", std::any(42)}});
    nid =
      gdb->add_node(":Person", {{"name", std::any(std::string("Ann"))},
                                  {"age", std::any(36)}});
    gdb->commit_transaction();
  }

  // we create a thread that tries to read "Ann"
  auto t1 = std::thread([&]() {
    gdb->begin_transaction();

    // inform tx #2 that we have started the transaction
    b1.notify();

    // wait until tx #2 has deleted the node.
    b2.wait();

    // spdlog::info("read node @{}", (unsigned long)&n);
    auto nd = gdb->get_node_description(nid);
    REQUIRE(nd.label == ":Person");
    REQUIRE(get_property<int>(nd.properties, "age") == 36);

    // now, tx #2 can commit
    b3.notify();
    gdb->commit_transaction();
  });

 // we create another thread that tries to delete "Ann"
  auto t2 = std::thread([&]() {
    // make sure that tx #1 has already started 
    b1.wait();

    gdb->begin_transaction();
    gdb->delete_node(nid);
    
    // inform tx #1 that we have deleted the node
    b2.notify();

    // wait until tx #1 has read the node
    b3.wait();
    gdb->commit_transaction();
  });
  t1.join();
  t2.join();

  graph_pool::destroy(pool);
}

TEST_CASE("Checking two concurrent transactions trying to create node", "[transaction]") {
  auto pool = graph_pool::create(test_path);
  auto gdb = pool->create_graph("my_tx_graph26");

  node::id_t nid1 = 0, nid2 = 0;

  // Thread 1: Add node
  auto t1 = std::thread([&]() {
    gdb->begin_transaction();

    nid1 = gdb->add_node("New Actor",
                        {{"name", std::any(std::string("Mark Wahlberg"))},
                         {"age", std::any(48)}});

    gdb->commit_transaction();
  });
  //  Thread 2: concurrently add node
  auto t2 = std::thread([&]() {
    gdb->begin_transaction();

	  nid2 = gdb->add_node("Actor",
	                        {{"name", std::any(std::string("Peter"))},
	                         {"age", std::any(22)}});

	  gdb->commit_transaction();
  });
  t1.join();
  t2.join();

  REQUIRE(gdb->get_nodes()->num_chunks() == 1);
  
  gdb->dump();
  graph_pool::destroy(pool);
} 
/* -------------------------------------------------------------------------------- */

