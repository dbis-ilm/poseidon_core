/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include "lru_list.hpp"

TEST_CASE("Testing lru_list", "[lru_list]") {
    SECTION("Create and add") {
        lru_list lst;
        lst.add_to_mru(1);
        lst.add_to_mru(2);
        lst.add_to_mru(3);

        REQUIRE(lst.size() == 3);
        REQUIRE(lst.lru() != nullptr);
        REQUIRE(lst.lru()->pid == 1);
    }

    SECTION("Add and remove") {
        lru_list lst;
        for (auto i = 1u; i <= 10; i++) {
            lst.add_to_mru(i);
        }

        REQUIRE(lst.size() == 10);

        for (auto i = 1u; i <= 10; i++) {
            auto pid = lst.remove_lru_node();
            REQUIRE(pid == i);
        }        

        REQUIRE(lst.size() == 0);

        REQUIRE(lst.remove_lru_node() == UNKNOWN);
    }
 
    SECTION("Move nodes") {
        lru_list lst;
        std::vector<lru_list::node *> nodes;
        for (auto i = 1u; i <= 10; i++) {
            nodes.push_back(lst.add_to_mru(i));
        }      

        lst.move_to_mru(nodes[4]);
        lst.move_to_mru(nodes[0]);
        lst.move_to_mru(nodes[9]);

        std::vector<uint64_t> pids = { 2, 3, 4, 6, 7, 8, 9, 5, 1, 10 };
        auto p = 0;
        for (auto iter = lst.begin(); iter != lst.end(); ++iter) {
            REQUIRE(*iter == pids[p++]);
        }
    }
}
