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

#include <cstdio>
#include <vector>

#include "catch.hpp"
#include "chunked_vec.hpp"

#ifdef USE_PMDK
using namespace pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

#endif

struct record {
  record() = default;
  record(record &&r) = default;
  record &operator=(record &&r) = default;

  record &operator=(const record &other) = default;

  uint8_t flag;
  uint32_t head;
  int64_t i;
  char s[44];
};

TEST_CASE("Testing chunked_vec", "[chunked_vec]") {
#ifdef USE_PMDK
  using vector_type = chunked_vec<record, DEFAULT_CHUNK_SIZE>;

  struct root {
    pmem::obj::persistent_ptr<vector_type> vec_p;
  };

  auto pop = pool<root>::create("/mnt/pmem0/poseidon/chunked_vec_test", "",
                                PMEMOBJ_POOL_SIZE);
  auto root_obj = pop.root();

  transaction::run(pop, [&] {
    root_obj->vec_p =
        make_persistent<vector_type>(/* optional constructor arguments */);
  });

  vector_type &vec = *(root_obj->vec_p);
#else
  chunked_vec<record, DEFAULT_CHUNK_SIZE> vec;
#endif

  SECTION("Adding some records") {
    std::cout << "Adding some records\n";
    // resize it to 16 chunks
    vec.resize(16);

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

    // check whether we can retrieve these records
    for (offset_t o = 0; o < 1000; o++) {
      const auto &rec = vec.const_at(o);
      REQUIRE(rec.flag == 1);
      REQUIRE(rec.head == o + 1);
      REQUIRE(rec.i == (int64_t)(o * 100 + 1));
      REQUIRE(strncmp(rec.s, "##########", 10) == 0);
    }
    vec.clear();
    REQUIRE(vec.capacity() == 0);
    REQUIRE(vec.num_chunks() == 0);

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

  SECTION("Adding and deleting some records") {
    // resize it to 16 chunks
    vec.resize(16);

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

    // mark some records as deleted
    std::vector<offset_t> victims = {5, 21, 64, 65, 125, 945};
    for (auto &v : victims) {
      vec.erase(v);
    }

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

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

  SECTION("Adding some records at scattered positions") {
    // resize it to 16 chunks
    vec.resize(16);

    // make sure we have enough space for 1000 records
    REQUIRE(vec.capacity() > 1000);

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

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

  SECTION("Adding many records and iterate over them") {
    // resize it to 16 chunks
    vec.resize(16);

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

    std::vector<offset_t> elems(1000);
    for (auto i = 0u; i < 1000; i++)
      elems[i] = i;

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

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

  SECTION("Adding and deleting some records followed by an iteration") {
    // resize it to 1 chunk
    vec.resize(1);

    // store 3 records in the array
    for (offset_t i = 0; i < 3; i++) {
      record rec;
      rec.head = i;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    // delete the first record
    vec.erase(0);

    // we expect only two records
    offset_t next = 1;
    for (auto &r : vec) {
      REQUIRE(r.head == next);
      next++;
    }

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

  SECTION("Adding many records and iterate over the chunks") {
    // resize it to 16 chunks
    vec.resize(16);

    // make sure we have enough space for 1000 records
    REQUIRE(vec.capacity() > 1000);
    std::cout << "#chunks = " << vec.num_chunks()
              << ", elems = " << vec.elements_per_chunk() << std::endl;
    // store 1000 records in the array
    for (offset_t i = 0; i < 1000; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }

    auto iter = vec.range(5, 10);
    offset_t first = 0, last = 320, num = 0;
    while (iter) {
      auto &rec = *iter;
      if (first == 0) {
        first = rec.head;
        REQUIRE(first == 321);
      }
      REQUIRE(last + 1 == rec.head);
      last = rec.head;
      num++;
      ++iter;
    }
    REQUIRE(num == 6 * 64);

#ifdef USE_PMDK
    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
#endif
  }

#ifdef USE_PMDK
  SECTION("Restoring persistent data") {
    // resize it to 16 chunks
    vec.resize(16);

    // make sure we have enough space for 1000 records
    REQUIRE(vec.capacity() > 1000);
    std::cout << "#chunks = " << vec.num_chunks()
              << ", elems = " << vec.elements_per_chunk() << std::endl;
    // store 1000 records in the array
    for (offset_t i = 0; i < 1000; i++) {
      record rec;
      rec.head = i + 1;
      rec.i = i * 100 + 1;
      memcpy(rec.s, "##########", 10);
      rec.flag = 1;
      vec.store_at(i, std::move(rec));
    }
    pop.close();
    // now, we reopen the pool and check whether the data is still there
    pop = pool<root>::open("/mnt/pmem0/poseidon/chunked_vec_test", "");
    root_obj = pop.root();
    vector_type &vec = *(root_obj->vec_p);

    REQUIRE(vec.capacity() > 1000);

    auto iter = vec.range(5, 10);
    offset_t first = 0, last = 320, num = 0;
    while (iter) {
      auto &rec = *iter;
      if (first == 0) {
        first = rec.head;
        REQUIRE(first == 321);
      }
      REQUIRE(last + 1 == rec.head);
      last = rec.head;
      num++;
      ++iter;
    }
    REQUIRE(num == 6 * 64);

    pop.close();
    remove("/mnt/pmem0/poseidon/chunked_vec_test");
  }
#endif
}
