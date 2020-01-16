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

#include "thread_pool.hpp"
#include <iostream>
#include "spdlog/spdlog.h"

void thread_pool::worker_thread() {
  while (!done) {
    function_wrapper task;
    // try to get the next task from the queue
    if (work_queue.try_pop(task)) {
      // and execute it
      task();
    } else {
      // give another thread the chance to put some work on the queue
      std::this_thread::yield();
    }
  }
}

thread_pool::thread_pool(size_t thread_count) : done(false), joiner(threads) {
  spdlog::debug("Creating thead_pool with {} threads", thread_count);
  try {
    // create a number of worker threads
    for (auto i = 0u; i < thread_count; ++i) {
      threads.push_back(std::thread(&thread_pool::worker_thread, this));
    }
  } catch (...) {
    done = true;
    throw;
  }
}

/**
 * Destructor.
 */
thread_pool::~thread_pool() { done = true; }