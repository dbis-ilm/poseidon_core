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

#include "benchmark/benchmark.h"
#include <cstdio>
#include <iostream>
#include <string>

#include "config.h"
#include "defs.hpp"
#include "dict.hpp"

#ifdef USE_PMDK

#define PMEMOBJ_POOL_SIZE                                                      \
  ((unsigned long long)(1024 * 1024 * 40000ull)) // 4000 MiB

using namespace pmem::obj;
const std::string bench_path = poseidon::gPmemPath + "bench";
#endif

class MyFixture : public benchmark::Fixture {
public:
  p_ptr<dict> dict_p;
#ifdef USE_PMDK
  pool_base pop;
#endif
  void SetUp(const ::benchmark::State &state) {
#ifdef USE_PMDK
    pop = pool_base::create(bench_path, "", PMEMOBJ_POOL_SIZE);
    transaction::run(pop, [&] { dict_p = p_make_ptr<dict>(); });
#else
    dict_p = p_make_ptr<dict>();
#endif
  }

  void TearDown(const ::benchmark::State &state) {
#ifdef USE_PMDK
    transaction::run(pop, [&] { delete_persistent<dict>(dict_p); });
    pop.close();
    remove(bench_path.c_str());
#else
    dict_p.reset();
#endif
  }
};

BENCHMARK_DEFINE_F(MyFixture, BM_DictInsert)(benchmark::State &state) {
  char buf[20];

  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
      sprintf(buf, "MyString%d", i);
      dict_p->insert(std::string(buf));
    }
  }
}

// Register the function as a benchmark
BENCHMARK_REGISTER_F(MyFixture, BM_DictInsert)->Range(8, 8 << 10);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_DictLookupString)(benchmark::State &state) {
  char buf[20];

  for (auto i = 0u; i < 10000; i++) {
    sprintf(buf, "MyString%d", i);
    dict_p->insert(std::string(buf));
  }
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
      sprintf(buf, "MyString%d", i);
      auto code = dict_p->lookup_string(std::string(buf));
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_DictLookupString)->Range(8, 8 << 10);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_DictLookupCode)(benchmark::State &state) {
  char buf[20];

  for (auto i = 0u; i < 10000; i++) {
    sprintf(buf, "MyString%d", i);
    dict_p->insert(std::string(buf));
  }
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
      auto str = dict_p->lookup_code(i);
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_DictLookupCode)->Range(8, 8 << 10);

BENCHMARK_MAIN();