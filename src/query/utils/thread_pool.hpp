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

#ifndef thread_pool_hpp_
#define thread_pool_hpp_

#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

#include "threadsafe_queue.hpp"

/**
 * Helper class to join a set of threads in the destructor.
 */
class join_threads {
  std::vector<std::thread> &threads;

public:
  explicit join_threads(std::vector<std::thread> &threads_)
      : threads(threads_) {}
  ~join_threads() {
    for (auto i = 0u; i < threads.size(); i++) {
      if (threads[i].joinable())
        threads[i].join();
    }
  }
};

class function_wrapper {
  struct impl_base {
    virtual void call() = 0;
    virtual ~impl_base() {}
  };

  std::unique_ptr<impl_base> impl;

  template <typename F> struct impl_type : impl_base {
    F f;
    impl_type(F &&f_) : f(std::move(f_)) {}
    void call() { f(); }
  };

public:
  template <typename F>
  function_wrapper(F &&f) : impl(new impl_type<F>(std::move(f))) {}
  function_wrapper() = default;
  function_wrapper(function_wrapper &&other) : impl(std::move(other.impl)) {}
  function_wrapper(const function_wrapper &) = delete;
  function_wrapper(function_wrapper &) = delete;

  void operator()() { impl->call(); }

  function_wrapper &operator=(function_wrapper &&other) {
    impl = std::move(other.impl);
    return *this;
  }

  function_wrapper &operator=(const function_wrapper &) = delete;
};

/**
 * An implementation of a thread pool based on Willians: C++ Concurrency in
 * Action.
 */
class thread_pool {
private:
  std::atomic_bool done;
  threadsafe_queue<function_wrapper> work_queue; // the jobs to be done
  std::vector<std::thread> threads;              // the list of workers
  join_threads joiner;

  /**
   * The actual worker thread.
   */
  void worker_thread();

public:
  /**
   * Constructor for creating the worker threads.
   */
  explicit thread_pool(
      size_t thread_count = std::thread::hardware_concurrency());

  /**
   * Destructor.
   */
  ~thread_pool();

  /**
   * Submit some work and push it to the queue.
   */
  template <typename FunctionType>
  std::future<typename std::invoke_result<FunctionType>::type>
  submit(FunctionType f) {
    using result_type = typename std::invoke_result<FunctionType>::type;

    std::packaged_task<result_type()> task(std::move(f));
    std::future<result_type> res(task.get_future());
    work_queue.push(std::move(task));
    return res;
  }
};

#endif