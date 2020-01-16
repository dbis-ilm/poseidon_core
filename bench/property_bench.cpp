/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#include "benchmark/benchmark.h"
#include <array>
#include <iostream>
#include <string>

#include "defs.hpp"
#include "dict.hpp"
#include "properties.hpp"

#ifdef USE_PMDK

#define PMEMOBJ_POOL_SIZE                                                      \
  ((unsigned long long)(1024 * 1024 * 40000ull)) // 4000 MiB

using namespace pmem::obj;
#endif

class MyFixture : public benchmark::Fixture {
public:
  p_ptr<dict> dict_p;
#ifdef USE_PMDK
  pool_base pop;
#endif
  void SetUp(const ::benchmark::State &state) {
#ifdef USE_PMDK
    pop = pool_base::create("/mnt/pmem0/poseidon/bench", "", PMEMOBJ_POOL_SIZE);
    transaction::run(pop, [&] { dict_p = p_make_ptr<dict>(); });
#else
    dict_p = p_make_ptr<dict>();
#endif
  }

  void TearDown(const ::benchmark::State &state) {
#ifdef USE_PMDK
    transaction::run(pop, [&] { delete_persistent<dict>(dict_p); });
    pop.close();
    remove("/mnt/pmem0/poseidon/bench");
#else
    dict_p.reset();
#endif
  }
};

static void BM_CreatePItem(benchmark::State &state) {
  for (auto _ : state) {
    p_item pi(42, 66.67);
  }
}

BENCHMARK(BM_CreatePItem);

/* ------------------------------------------------------------- */

static void BM_CreatePItemArray(benchmark::State &state) {
  for (auto _ : state) {
    std::array<p_item, 10> items;
    for (auto i = 0u; i < 10; i++)
      items[i] = p_item(i, i * 10.5);
  }
}

BENCHMARK(BM_CreatePItemArray);

/* ------------------------------------------------------------- */

static void BM_AccessDoublePItem(benchmark::State &state) {
  p_item pi(42, 66.67);
  for (auto _ : state) {
    double d = pi.get<double>();
  }
}

BENCHMARK(BM_AccessDoublePItem);

/* ------------------------------------------------------------- */

static void BM_AccessIntPItem(benchmark::State &state) {
  p_item pi(42, 69);
  for (auto _ : state) {
    double d = pi.get<int>();
  }
}

BENCHMARK(BM_AccessIntPItem);

/* ------------------------------------------------------------- */

static void BM_ComparePItem(benchmark::State &state) {
  p_item pi1(42, 66.67);
  p_item pi2(42, 66);
  bool res;
  for (auto _ : state) {
    res = pi1.equal(66.67);
    res = pi1.equal(66);
    res = pi2.equal(66.67);
    res = pi2.equal(66);
  }
}

BENCHMARK(BM_ComparePItem);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_IntAnyToPItem)(benchmark::State &state) {
  for (auto _ : state) {
    p_item pi(boost::any((int)42), dict_p);
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_IntAnyToPItem);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_StringAnyToPItem)(benchmark::State &state) {
  for (auto _ : state) {
    p_item pi(boost::any(std::string("\"hello\"")), dict_p);
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_StringAnyToPItem);

/* ------------------------------------------------------------- */

BENCHMARK_MAIN();
