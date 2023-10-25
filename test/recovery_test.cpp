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
#include "defs.hpp"
#include "graph_db.hpp"
#include "graph_pool.hpp"
#include "query_ctx.hpp"

#include <boost/process.hpp>

namespace bp = boost::process;

const std::string test_path = PMDK_PATH("recovery_tst");

TEST_CASE("Recovery of aborted inserts using WAL", "[graph_db]") {
    char buf[1024];
    remove(test_path.c_str());
    spdlog::debug("getcwd {}", getcwd(buf, 1024)); 
    std::string prog(buf);
    prog += "/abort_insert";
    spdlog::info("exec prog '{}'", prog);
    int result = bp::system(prog, test_path, "my_graph");
    REQUIRE(result == 0);

    spdlog::info("recovery_test: reopen database...");

    auto pool = graph_pool::open(test_path);
    auto graph = pool->open_graph("my_graph");
    query_ctx ctx(graph);

    // check that we have one insert
    ctx.run_transaction([&]() {
    	int num_nodes = 0;
    	ctx.nodes([&](auto& n) { num_nodes++; });
    	REQUIRE(num_nodes == 1);

    	// but also that the slots are not occupied
    	auto& n = graph->node_by_id(0);
        std::cout << "node = " << graph->get_node_description(n.id()) << std::endl;
        // std::cout << "node = " << n.id() << std::endl;
 	    return false;
    });

    graph_pool::destroy(pool);
}
