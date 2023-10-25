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
#include "graph_pool.hpp"


const std::string test_path = PMDK_PATH("gpool_tst");

TEST_CASE("Creating a pool", "[graph_pool]") {
    auto pool = graph_pool::create(test_path);
    REQUIRE(pool.get() != nullptr);

    node::id_t nid;

    auto graph = pool->create_graph("my_pool_graph1");
    // add a node
    {
        graph->begin_transaction();

        nid = graph->add_node("Person", {{"name", std::any(std::string("Anne"))},
                                  {"age", std::any(28)}});
        graph->commit_transaction();
    }
    // check that the node exists
    {
        graph->begin_transaction();

        graph->node_by_id(nid);
        auto nd = graph->get_node_description(nid);
        REQUIRE(nd.id == nid);
        REQUIRE(nd.label == "Person");

        graph->commit_transaction();
    }
    pool->close();
    spdlog::info("------ try to reopen graph_pool ... ------");
    pool = graph_pool::open(test_path);
    auto graph2 = pool->open_graph("my_pool_graph1");

    // check the node
    {
        graph2->begin_transaction();

        graph2->node_by_id(nid);
        auto nd = graph2->get_node_description(nid);
        REQUIRE(nd.id == nid);
        REQUIRE(nd.label == "Person");

        graph2->commit_transaction();
    }
    REQUIRE_THROWS_AS(pool->open_graph("your_graph"), unknown_db);

    graph_pool::destroy(pool);
    REQUIRE(access(test_path.c_str(), F_OK) == -1);
}
