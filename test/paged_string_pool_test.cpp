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

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do
                           // this in one cpp file

#include <cstdio>
#include <vector>
#include <iostream>
#include <strstream>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>
#include "paged_string_pool.hpp"

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

TEST_CASE("Adding some strings", "[paged_string_pool]") {
    create_dir("spool_test1");
    const uint8_t file_id = 0;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("spool_test1/dict.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    paged_string_pool spool(bpool, file_id);

    std::vector<dcode_t> codes(100000);

    for (auto i = 0u; i < 100000; i++) {
        std::ostrstream os;
        os << "This_is_a_String:" << i << std::ends;
        auto code = spool.add(os.str());
        codes[i] = code;
    }

    for (auto i = 0u; i < 100000; i++) {
        std::ostrstream os;
        os << "This_is_a_String:" << i << std::ends;
        auto s = spool.extract(codes[i]);
        REQUIRE(strcmp(s, os.str()) == 0);
    }

    delete_dir("spool_test1");
}

TEST_CASE("Adding some strings and restore the string pool", "[paged_string_pool]") {
    create_dir("spool_test2");
    const uint8_t file_id = 5;
    std::vector<dcode_t> codes(100000);

    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test2/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);

        for (auto i = 0u; i < 100000; i++) {
            std::ostrstream os;
            os << "This_is_a_String:" << i << std::ends;
            auto code = spool.add(os.str());
            codes[i] = code;
        }
        bpool.flush_all();
    }

     {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test2/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);
   
        for (auto i = 0u; i < 100000; i++) {
            std::ostrstream os;
            os << "This_is_a_String:" << i << std::ends;
            auto s = spool.extract(codes[i]);
            REQUIRE(strcmp(s, os.str()) == 0);
        }
    }

    delete_dir("spool_test2");
}

TEST_CASE("Testing equal in a restored string pool", "[paged_string_pool]") {
    create_dir("spool_test3");
    const uint8_t file_id = 5;
    std::vector<dcode_t> codes(100000);

    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test3/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);

        for (auto i = 0u; i < 100000; i++) {
            std::ostrstream os;
            os << "This_is_a_String:" << i << std::ends;
            auto code = spool.add(os.str());
            codes[i] = code;
        }
        bpool.flush_all();
    }

     {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test3/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);
   
        for (auto i = 0u; i < 100000; i++) {
            std::ostrstream os;
            os << "This_is_a_String:" << i << std::ends;
            REQUIRE(spool.equal(codes[i], os.str()));
        }
    }

    delete_dir("spool_test3");
}

TEST_CASE("Testing the scan of a string pool", "[paged_string_pool]") {
    create_dir("spool_test4");
    const uint8_t file_id = 5;
    std::unordered_map<dcode_t, std::string> stable;

    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test4/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);

        for (auto i = 0u; i < 1000; i++) {
            std::ostrstream os;
            os << "This_is_a_String:" << i << std::ends;
            auto code = spool.add(os.str());
            stable[code] = os.str();
        }
        bpool.flush_all();
    }

     {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("spool_test4/dict.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        paged_string_pool spool(bpool, file_id);
   
        int num = 0;
        spool.scan([&](const char *s, dcode_t c) {
            REQUIRE(stable[c] == s);
            num++;
        });
        REQUIRE(num == 1000);
    }

    delete_dir("spool_test4");
}