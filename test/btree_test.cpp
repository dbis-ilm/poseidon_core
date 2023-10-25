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

#include <set>

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "graph_db.hpp"
#include "graph_pool.hpp"
#include "btree.hpp"

#include <filesystem>

const std::string test_path = PMDK_PATH("btree_tst");

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

void create_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj))
        std::filesystem::create_directory(path_obj);
}

void delete_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    std::filesystem::remove_all(path_obj);
}

TEST_CASE("Creating an btree index", "[btree]") {
    create_dir("btree_test1");
    const uint8_t file_id = 6;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("btree_test1/btree.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    auto mybtree = make_pf_btree(bpool, file_id);

    mybtree->insert(42u, 12);

    offset_t val;
    REQUIRE(mybtree->lookup(42u, &val));
    REQUIRE(val == 12);

    REQUIRE(!mybtree->lookup(43u, &val));

    delete_dir("btree_test1");
}

TEST_CASE("Creating a persistent btree index", "[btree]") {

    create_dir("btree_test2");
    const uint8_t file_id = 6;
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("btree_test2/btree.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        auto mybtree = make_pf_btree(bpool, file_id);
    
        mybtree->insert(42u, 12);
        mybtree->insert(45u, 14);
        mybtree->insert(43u, 13);
        mybtree->insert(47u, 15);

        offset_t val;
        REQUIRE(mybtree->lookup(42u, &val));
        REQUIRE(val == 12);
        mybtree->print();
        bpool.flush_all();
    }
    {
        std::cout << "reopen btree.." << std::endl;
        auto test_file = std::make_shared<paged_file>();
        test_file->open("btree_test2/btree.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        auto mybtree = make_pf_btree(bpool, file_id);
        mybtree->print();

        offset_t val;
        REQUIRE(mybtree->lookup(42u, &val));
        REQUIRE(val == 12);
        
        REQUIRE(mybtree->lookup(47u, &val));
        REQUIRE(val == 15);
    }
    delete_dir("btree_test2");
}

TEST_CASE("Creating a btree index with multiple levels", "[btree]") {
    create_dir("btree_test3");
    const uint8_t file_id = 6;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("btree_test3/btree.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    auto mybtree = make_pf_btree(bpool, file_id);

    for (auto i = 1u; i < 300; i++)
        mybtree->insert(i, i + 1000);

    // mybtree->print();

    offset_t val;
    for (auto i = 1u; i < 300; i++) {
        REQUIRE(mybtree->lookup(i, &val));
        REQUIRE(val == i + 1000);
    }
    bpool.flush_all();
    delete_dir("btree_test3");
}

TEST_CASE("Creating a persistent btree index with multiple levels", "[btree]") {
    create_dir("btree_test4");
    const uint8_t file_id = 6;
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("btree_test4/btree.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        auto mybtree = make_pf_btree(bpool, file_id);

        for (auto i = 1u; i < 300; i++)
            mybtree->insert(i, i + 1000);

        bpool.flush_all();
        // mybtree->print();
    }
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("btree_test4/btree.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        auto mybtree = make_pf_btree(bpool, file_id);
        // mybtree->print();

        offset_t val;
        for (auto i = 1u; i < 300; i++) {
            REQUIRE(mybtree->lookup(i, &val));
            REQUIRE(val == i + 1000);
        }
        auto k = 1u;
        mybtree->scan([&](const auto &key, const auto &val) {
            REQUIRE(k == key);
            REQUIRE(val == k + 1000);
            k++;
        });
    }
    delete_dir("btree_test4");
}

TEST_CASE("Creating an btree index, insert items and delete some of them", "[btree]") {
    create_dir("btree_test5");
    const uint8_t file_id = 6;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("btree_test5/btree.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    auto mybtree = make_pf_btree(bpool, file_id);

    for (auto i = 1u; i < 1000; i++)
        mybtree->insert(i, i + 1000);

    for (auto i = 20u; i < 1000; i += 50)
       mybtree->erase(i);

    auto k = 1u;
    mybtree->scan([&](const auto &key, const auto &val) {
        REQUIRE(k == key);
        REQUIRE(val == k + 1000);
        k++;
        if (k % 50 == 20)
            k++;
    });

    bpool.flush_all();
    delete_dir("btree_test5");
}
