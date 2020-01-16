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
#include <boost/signals2.hpp>
#include <functional>
#include <iostream>
#include <memory>

namespace v1 {
struct test_op;
using op_ptr = std::shared_ptr<test_op>;

struct test_op {
  test_op() = default;
  virtual ~test_op() = default;

  virtual void func() {
    if (subscriber)
      subscriber->func();
  }

  op_ptr subscriber;
};

struct sub_op : public test_op {
  sub_op() = default;
  virtual ~sub_op() = default;

  virtual void func() override { var = 42; }
  int var;
};

} // namespace v1

static void BM_VirtualFunc(benchmark::State &state) {
  using namespace v1;
  auto root = std::make_shared<test_op>();
  root->subscriber = std::make_shared<sub_op>();
  for (auto _ : state) {
    root->func();
  }
}

BENCHMARK(BM_VirtualFunc);

/* ------------------------------------------------------------- */

namespace v2 {

struct test_op {
  test_op() = default;
  ~test_op() = default;

  void func() { sfunc(); }

  std::function<void()> sfunc;
};

struct sub_op : public test_op {
  sub_op() = default;
  ~sub_op() = default;

  void my_func() { var = 42; }
  int var;
};

} // namespace v2

static void BM_FuncPtr(benchmark::State &state) {
  using namespace v2;
  auto root = std::make_shared<test_op>();
  auto sub = std::make_shared<sub_op>();
  root->sfunc = std::bind(&sub_op::my_func, sub.get());
  for (auto _ : state) {
    root->func();
  }
}

BENCHMARK(BM_FuncPtr);

/* ------------------------------------------------------------- */

namespace v3 {

struct test_op {
  test_op() = default;
  ~test_op() = default;

  void func() { sig(); }

  boost::signals2::signal<void()> sig;
};

struct sub_op : public test_op {
  sub_op() = default;
  ~sub_op() = default;

  void my_func() { var = 42; }
  int var;
};

} // namespace v3

static void BM_Signal(benchmark::State &state) {
  using namespace v3;
  auto root = std::make_shared<test_op>();
  auto sub = std::make_shared<sub_op>();
  root->sig.connect(std::bind(&sub_op::my_func, sub.get()));
  for (auto _ : state) {
    root->func();
  }
}

BENCHMARK(BM_Signal);

/* ------------------------------------------------------------- */

BENCHMARK_MAIN();