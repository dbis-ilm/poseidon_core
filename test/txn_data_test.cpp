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
#include "nodes.hpp"

TEST_CASE("Test creation of a txn_data object"  "[txn_data]") {  
    node n;
    transaction tx;

    REQUIRE(n.bts() == 0);
    REQUIRE(n.cts() == INF);
    REQUIRE(n.rts() == 0);
    n.set_timestamps(0, tx.xid());

    REQUIRE(n.bts() == 0);
    REQUIRE(n.cts() == tx.xid());
    REQUIRE(n.rts() == 0);

    n.set_rts(42);
    REQUIRE(n.rts() == 42);

    n.set_cts(INF);
    REQUIRE(n.cts() == INF);

    n.set_cts(tx.xid());

    REQUIRE(n.txn_id() == 0);

    n.set_timestamps(50, 100);
    REQUIRE(!n.is_valid());
    REQUIRE(n.is_valid_for(70));

    n.set_timestamps(100, INF);
    REQUIRE(n.is_valid());
    REQUIRE(n.is_valid_for(tx.xid()));
}

TEST_CASE("Test locking of a txn_data object"  "[txn_data]") {  
    node n;
    transaction tx1;

    REQUIRE(!n.is_locked());
    n.lock(tx1.xid());
    REQUIRE(n.txn_id() == tx1.xid());
    REQUIRE(n.is_locked());
    REQUIRE(n.is_locked_by(tx1.xid()));
    REQUIRE(!n.is_locked_by(42));

    transaction tx2;
    REQUIRE(!n.try_lock(tx2.xid()));

    n.unlock();
    REQUIRE(!n.is_locked());
    REQUIRE(n.try_lock(tx2.xid()));
}

TEST_CASE("Test versioning of a txn_data object"  "[txn_data]") {  
    node n(42);
    std::list<p_item> dirty_list;

    REQUIRE(n.node_label == 42);
    REQUIRE(!n.is_dirty());
    REQUIRE(!n.has_dirty_versions());

    auto& dv1 = n.add_dirty_version(std::make_unique<dirty_node>(n, dirty_list));
    dv1->elem_.set_timestamps(20, 50);
    dv1->elem_.set_dirty();
    REQUIRE(dv1->elem_.is_dirty());
    REQUIRE(n.has_dirty_versions());

    auto& dv2 = n.add_dirty_version(std::make_unique<dirty_node>(n, dirty_list));
    dv2->elem_.set_timestamps(50, 100);
    dv2->elem_.set_dirty();
 
    auto& dv3 = n.add_dirty_version(std::make_unique<dirty_node>(n, dirty_list));
    dv3->elem_.set_timestamps(100, INF);
    dv3->elem_.set_dirty();
 
    REQUIRE(!n.has_valid_version(10));
    REQUIRE(n.has_valid_version(40));
    REQUIRE(n.has_valid_version(150));
 
    REQUIRE_THROWS_AS(n.find_valid_version(10), transaction_abort);
 
    auto& nv1 = n.find_valid_version(25);
    REQUIRE(nv1->elem_.node_label == 42);

    auto& nv2 = n.find_valid_version(150);
    REQUIRE(nv2->elem_.node_label == 42);

    // TODO: fill dirty_list and test properties
}