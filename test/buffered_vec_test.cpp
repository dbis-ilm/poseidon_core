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
#include <filesystem>

#include <catch2/catch_test_macros.hpp>
#include "buffered_vec.hpp"

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

struct record {
    record() = default;
    record(record &&r) = default;
    record &operator=(record &&r) = default;

    record &operator=(const record &other) = default;

    void runtime_initialize() {}
    
    uint8_t flag;
    uint32_t head;
    int64_t i;
    char s[44];
};

TEST_CASE("Adding some records", "[buffered_vec]") {
    create_dir("bv_test1");
    const uint8_t file_id = 5;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("bv_test1/bv_records.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    buffered_vec<record> vec(bpool, file_id);
    vec.resize(10);
    std::cout << "nelems = " << vec.elements_per_chunk() << std::endl;

    // make sure we have enough space for 1000 records
    REQUIRE(vec.capacity() > 1000);

    CHECK_THROWS_AS(vec.at(1000000), index_out_of_range);

    // store 1000 records in the array
    for (offset_t i = 0; i < 1000; i++) {
        record rec;
        rec.head = i + 1;
        rec.i = i * 100 + 1;
        memcpy(rec.s, "##########", 10);
        rec.flag = 11;
        vec.store_at(i, std::move(rec));
    }

    // check whether we can retrieve these records
    for (offset_t o = 0; o < 1000; o++) {
        const auto &rec = vec.const_at(o);
        REQUIRE(rec.flag == 11);
        REQUIRE(rec.head == o + 1);
        REQUIRE(rec.i == (int64_t)(o * 100 + 1));
        REQUIRE(strncmp(rec.s, "##########", 10) == 0);
    }
    bpool.flush_all();
    delete_dir("bv_test1");
}

TEST_CASE("Adding some records, close the file, and reopen it", "[buffered_vec]") {
    create_dir("bv_test2");
    const uint8_t file_id = 0;
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bv_test2/bv_records.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        buffered_vec<record> vec(bpool, file_id);
        vec.resize(2);

        // store 30000 records in the array
        for (offset_t i = 0; i < 30000; i++) {
            record rec;
            rec.head = i + 1;
            rec.i = i * 100 + 1;
            memcpy(rec.s, "##########", 10);
            rec.flag = 11;
            vec.store_at(i, std::move(rec));
        }
        bpool.flush_all();
    }
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bv_test2/bv_records.db", file_id);
        REQUIRE(test_file->is_open());

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        buffered_vec<record> vec2(bpool, file_id);
        for (offset_t o = 0; o < 30000; o++) {
            const auto &rec = vec2.const_at(o);
            REQUIRE(rec.flag == 11);
            REQUIRE(rec.head == o + 1);
            REQUIRE(rec.i == (int64_t)(o * 100 + 1));
            REQUIRE(strncmp(rec.s, "##########", 10) == 0);
        }
    }
    delete_dir("bv_test2");
}

TEST_CASE("Adding and deleting some records, close the file, and reopen it", "[buffered_vec]") {
    create_dir("bv_test3");
    std::vector<offset_t> victims = {5, 21, 64, 65, 125, 945};
    const uint8_t file_id = 0;
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bv_test3/bv_records.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        buffered_vec<record> vec(bpool, file_id);
        vec.resize(2);

        // store 1000 records in the array
        for (offset_t i = 0; i < 1000; i++) {
            record rec;
            rec.head = i + 1;
            rec.i = i * 100 + 1;
            memcpy(rec.s, "##########", 10);
            rec.flag = 1;
            vec.store_at(i, std::move(rec));
        }
        bpool.flush_all();
      }
    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bv_test3/bv_records.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        buffered_vec<record> vec(bpool, file_id);
   
        // mark some records as deleted
        for (auto &v : victims) {
            vec.erase(v);
        }
        bpool.flush_all();
    }

    {
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bv_test3/bv_records.db", file_id);

        bufferpool bpool;
        bpool.register_file(file_id, test_file);

        buffered_vec<record> vec(bpool, file_id);

        // check whether we can retrieve these records
        std::size_t v = 0;
        for (offset_t o = 0; o < 1000; o++) {
            try {
                const auto &rec = vec.const_at(o);
                REQUIRE(rec.flag != 0);
                REQUIRE(rec.head == o + 1);
                REQUIRE(rec.i == (int64_t)(o * 100 + 1));
                REQUIRE(strncmp(rec.s, "##########", 10) == 0);
            } catch (unknown_id &exc) {
                // make sure the record is from the delete list
                std::cout << "out_of_range: " << o << std::endl;
                REQUIRE(victims[v++] == o);
            }
        }
        REQUIRE(v == victims.size());
    }
    delete_dir("bv_test3");
}

TEST_CASE("Adding some records at scattered positions", "[buffered_vec]") {
    create_dir("bv_test4");
    const uint8_t file_id = 0;
        
    auto test_file = std::make_shared<paged_file>();
    test_file->open("bv_test4/bv_records.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    buffered_vec<record> vec(bpool, file_id);
    vec.resize(1);

    for (offset_t i = 0; i < 100; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    for (offset_t i = 102; i < 150; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    for (offset_t i = 157; i < 300; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }
    
    REQUIRE(vec.first_available() == 100);

    {
      auto i = 100;
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    REQUIRE(vec.first_available() == 101);
    {
      auto i = 101;
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }
    REQUIRE(vec.first_available() == 150);

    delete_dir("bv_test4");
}

TEST_CASE("Adding many records and iterate over them", "[buffered_vec]") {
    create_dir("bv_test5");
    const uint8_t file_id = 0;
        
    auto test_file = std::make_shared<paged_file>();
    test_file->open("bv_test5/bv_records.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    buffered_vec<record> vec(bpool, file_id);
    vec.resize(2);

    // make sure we have enough space for 1000 records
    REQUIRE(vec.capacity() > 1000);

    // store 1000 records in the array
    for (offset_t i = 0; i < 1000; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    offset_t o = 0;
    for (auto &rec : vec) {
      REQUIRE(rec.flag == 1);
      REQUIRE(rec.head == o + 1);
      REQUIRE(rec.i == (int64_t)(o * 100 + 1));
      REQUIRE(strncmp(rec.s, "##########", 10) == 0);
      o++;
    }
    REQUIRE(o == 1000);

    std::vector<offset_t> elems(1000);
    for (auto i = 0u; i < 1000; i++) elems[i] = i;

    // delete some records
    std::vector<offset_t> victims = {945, 125, 65, 64, 21, 5};
    for (auto &v : victims) {
      vec.erase(v);
      elems.erase(elems.begin() + v);
    }

    o = 0;
    for (auto &rec : vec) {
      REQUIRE(rec.flag == 1);
      REQUIRE(rec.head == elems[o] + 1);
      REQUIRE(rec.i == (int64_t)(elems[o] * 100 + 1));
      REQUIRE(strncmp(rec.s, "##########", 10) == 0);
      o++;
    }

    delete_dir("bv_test5");
}

TEST_CASE("Append many records", "[buffered_vec]") {
    create_dir("bv_test6");
    const uint8_t file_id = 0;
        
    auto test_file = std::make_shared<paged_file>();
    test_file->open("bv_test6/bv_records.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    buffered_vec<record> vec(bpool, file_id);

    // store 30000 records in the array
    for (offset_t i = 0; i < 30000; i++) {
        record rec;
        rec.head = i + 1;
        rec.i = i * 100 + 1;
        memcpy(rec.s, "##########", 10);
        rec.flag = 11;
        auto res = vec.append(std::move(rec));
        REQUIRE(res.first == i);
        REQUIRE(res.second != nullptr);
    }
    bpool.flush_all();

    delete_dir("bv_test6");
}

#if 0

TEST_CASE("Create a vector with many records and test the freespace management", "[buffered_vec]") {
    create_dir("bv_test7");
    const uint8_t file_id = 0;
        
    auto test_file = std::make_shared<paged_file>();
    test_file->open("bv_test7/bv_records.db", file_id);

    bufferpool bpool;
    bpool.register_file(file_id, test_file);

    buffered_vec<record> vec(bpool, file_id);

    // store 1.000.000 records in the array
    for (offset_t i = 0; i < 1000000; i++) {
        record rec;
        rec.head = i + 1;
        rec.i = i * 100 + 1;
        memcpy(rec.s, "##########", 10);
        rec.flag = 11;
        auto res = vec.append(std::move(rec));
        REQUIRE(res.first == i);
        REQUIRE(res.second != nullptr);
    }
    bpool.flush_all();
    std::cout << "number of chunks: " << vec.num_chunks() << std::endl;
    // delete a number of records from different chunks
    std::vector<offset_t> victims = {16000, 45000, 45001, 45002, 161290, 161291};
    for (auto &v : victims) {
        vec.erase(v);
    }
    // check that chunks are reused if new records are inserted
    REQUIRE(vec.first_available() == 16000);
    {
        record rec;
        rec.head = 2000000;
        rec.i = 2000000;
        memcpy(rec.s, "##########", 10);
        rec.flag = 12;
        auto res = vec.store(std::move(rec));
        REQUIRE(res.first == 16000);
        REQUIRE(res.second != nullptr);

    }
    delete_dir("bv_test7");
}

#endif
