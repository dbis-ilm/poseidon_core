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
#include <iostream>

#include "defs.hpp"
#include "graph_db.hpp"

#ifdef USE_PMDK

#define PMEMOBJ_POOL_SIZE                                                      \
  ((unsigned long long)(1024 * 1024 * 40000ull)) // 4000 MiB

namespace nvm = pmem::obj;
#endif

class MyFixture : public benchmark::Fixture {
public:
  graph_db_ptr graph;
#ifdef USE_PMDK
  nvm::pool_base pop;
#endif
  void SetUp(const ::benchmark::State &state) {
#ifdef USE_PMDK
    pop = nvm::pool_base::create("/mnt/pmem0/poseidon/bench", "", PMEMOBJ_POOL_SIZE);
    nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
    graph = p_make_ptr<graph_db>();
#endif
  }

  void TearDown(const ::benchmark::State &state) {
#ifdef USE_PMDK
    nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
    pop.close();
    remove("/mnt/pmem0/poseidon/bench");
#else
    graph.reset();
#endif
  }
};

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_AppendNode)(benchmark::State &state) {
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto p1 = graph->add_node("Person",
                                {{"name", boost::any(std::string("John Doe"))},
                                 {"age", boost::any(42)},
                                 {"number", boost::any(i)},
                                 {"dummy1", boost::any(std::string("Dummy"))},
                                 {"dummy2", boost::any(1.2345)}},
                                true);
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_AppendNode)->Range(8, 8 << 12);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_CreateNode)(benchmark::State &state) {
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto p1 = graph->add_node("Person",
                                {{"name", boost::any(std::string("John Doe"))},
                                 {"age", boost::any(42)},
                                 {"number", boost::any(i)},
                                 {"dummy1", boost::any(std::string("Dummy"))},
                                 {"dummy2", boost::any(1.2345)}});
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_CreateNode)->Range(8, 8 << 12);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_AppendBlankNode)(benchmark::State &state) {
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto p1 = graph->add_node("Person", {}, true);
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_AppendBlankNode)->Range(8, 8 << 12);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_CreateBlankNode)(benchmark::State &state) {
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto p1 = graph->add_node("Person", {});
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_CreateBlankNode)->Range(8, 8 << 12);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_GetNodeById)(benchmark::State &state) {
#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif
  for (int i = 0u; i < 10000; i++) {
    auto p1 = graph->add_node("Person", {}, true);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto &n = graph->node_by_id((node::id_t)i);
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_GetNodeById)->Range(8, 8 << 10);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_GetNodeDescription)(benchmark::State &state) {
  for (int i = 0u; i < 10000; i++) {
#ifdef USE_TX
    auto tx = graph->begin_transaction();
#endif
    auto p1 = graph->add_node("Person",
                              {{"name", boost::any(std::string("John Doe"))},
                               {"age", boost::any(42)},
                               {"number", boost::any(i)},
                               {"dummy1", boost::any(std::string("Dummy"))},
                               {"dummy2", boost::any(1.2345)}},
                              true);
#ifdef USE_TX
    graph->commit_transaction();
#endif
  }
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      auto &n = graph->node_by_id((node::id_t)i);
      auto ndescr = graph->get_node_description(n);
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_GetNodeDescription)->Range(8, 8 << 6);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_ScanNodes)(benchmark::State &state) {
#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif
  for (int i = 0u; i < 10000; i++) {
    auto p1 = graph->add_node("Person",
                              {{"name", boost::any(std::string("John Doe"))},
                               {"age", boost::any(42)},
                               {"number", boost::any(i)},
                               {"dummy1", boost::any(std::string("Dummy"))},
                               {"dummy2", boost::any(1.2345)}},
                              true);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      graph->nodes([](node &n) { auto id_ = n.id(); });
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_ScanNodes)->Range(8, 8 << 4);

/* ------------------------------------------------------------- */

BENCHMARK_DEFINE_F(MyFixture, BM_ScanNodesByLabel)(benchmark::State &state) {
#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif
  for (int i = 0u; i < 10000; i++) {
    auto p1 = graph->add_node("Person",
                              {{"name", boost::any(std::string("John Doe"))},
                               {"age", boost::any(42)},
                               {"number", boost::any(i)},
                               {"dummy1", boost::any(std::string("Dummy"))},
                               {"dummy2", boost::any(1.2345)}},
                              true);
  }
#ifdef USE_TX
  graph->commit_transaction();
#endif
  for (auto _ : state) {
    for (int i = 0u, i_end = state.range(0); i < i_end; i++) {
#ifdef USE_TX
      auto tx = graph->begin_transaction();
#endif
      graph->nodes_by_label("Person", [](node &n) { auto id_ = n.id(); });
#ifdef USE_TX
      graph->commit_transaction();
#endif
    }
  }
}

BENCHMARK_REGISTER_F(MyFixture, BM_ScanNodesByLabel)->Range(8, 8 << 4);

/* ------------------------------------------------------------- */

BENCHMARK_MAIN();