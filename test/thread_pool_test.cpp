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

#include <vector>
#include <numeric>

#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>

#include <catch2/catch_test_macros.hpp>
#include "thread_pool.hpp"

using namespace std::chrono_literals;

#if 0
template<typename Iterator, typename T>
struct accumulate_block {
    T operator()(Iterator first, Iterator last) {
        return std::accumulate(first, last, T());
    }
};

template<typename Iterator, typename T>
T parallel_accumulate(Iterator first, Iterator last, T init) {
    unsigned long const length = std::distance(first, last);

    if (!length)
        return init;

    unsigned long const block_size = 25;
    unsigned long const num_blocks = (length + block_size-1) / block_size;

    std::vector<std::future<T>> futures(num_blocks-1);
    thread_pool pool;

    Iterator block_start = first;
    for (auto i = 0u; i < (num_blocks-1); i++) {
        Iterator block_end = block_start;
        std::advance(block_end, block_size);
        futures[i] = pool.submit(accumulate_block<Iterator, T>() (block_start, block_end));
        block_start = block_end;
    }
    T last_result = accumulate_block<Iterator, T>() (block_start, last);
    T result = init;

    for (auto i = 0u; i < (num_blocks-1); i++) {
        result += futures[i].get();
    }
    result += last_result;
    return result;
}

TEST_CASE("Testing thread pool", "[thread_pool]") {
    std::vector<int> v(1000000);
    for (auto i = 1u; i <= 1000000; i++) {
        v[i] = i;
    }
    int sum1 = std::accumulate(v.begin(), v.end(), 0);
    int sum2 = parallel_accumulate(v.begin(), v.end(), 0);
    REQUIRE(sum1 == sum2);
}
#endif

// Test cases from https://github.com/f-squirrel/thread_pool

struct Task {
    Task(std::set<std::thread::id>& s, std::mutex& m): ids_(s), mutex_(m) {}
    void operator()() {
        {
            std::unique_lock<std::mutex> lock{mutex_};
            ids_.insert(std::this_thread::get_id());
        }
        std::this_thread::sleep_for(1s);
    }
    std::set<std::thread::id>& ids_;
    std::mutex& mutex_;
};

TEST_CASE("Run more tasks than threads", "[thread_pool]") {

    std::mutex mutex;
    std::set<std::thread::id> set;
    const size_t THREAD_COUNT{2u};
    const size_t TASK_COUNT{20u};
    std::vector<std::future<typename std::invoke_result<Task>::type>> v;
    {
        thread_pool pool{THREAD_COUNT};
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            v.push_back(pool.submit(Task(set, mutex)));
        }

        std::this_thread::sleep_for(1s);
        REQUIRE(v.size() == TASK_COUNT);
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            v[i].get();
        }
    }
}

struct IntegerTask : public Task {
	IntegerTask(std::set<std::thread::id>& s, std::mutex& m) : Task(s, m) {}
	std::thread::id operator()() {
		{
			std::unique_lock<std::mutex> lock{mutex_};
			ids_.insert(std::this_thread::get_id());
		}
		std::this_thread::sleep_for(1s);
		return std::this_thread::get_id();
	}
};

TEST_CASE("Return integers", "[thread_pool]") {
    std::mutex mutex;
    std::set<std::thread::id> set;
    const size_t TASK_COUNT{4u};
    std::vector<std::future<typename std::invoke_result<IntegerTask>::type>> v;
    {
        thread_pool pool{2u};
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            v.push_back(pool.submit(IntegerTask(set, mutex)));
        }

        std::this_thread::sleep_for(1s);
        REQUIRE(v.size() == TASK_COUNT);
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            std::cout << v[i].get() << "\n";
        }
    }
}

struct StringTask {
    StringTask(std::set<std::thread::id>& s, std::mutex& m): ids_(s), mutex_(m) {}
	std::string operator()() {
		const auto id = std::this_thread::get_id();
        {
            std::unique_lock<std::mutex> lock{mutex_};
            ids_.insert(id);
        }
        std::this_thread::sleep_for(1s);
		return "hash string: " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    }
    std::set<std::thread::id>& ids_;
    std::mutex& mutex_;
};

TEST_CASE("Various types of tasks", "[thread_pool]") {
    std::mutex mutex;
    std::set<std::thread::id> set;
    const size_t TASK_COUNT{4u};
    std::vector<std::future<typename std::invoke_result<StringTask>::type>> v;
    {
        thread_pool pool{4u};
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            pool.submit(IntegerTask(set, mutex));
        }
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            v.push_back(pool.submit(StringTask(set, mutex)));
        }
        std::this_thread::sleep_for(1s);
        for(size_t i = 0; i < TASK_COUNT; ++i) {
            std::cout << v[i].get() << "\n";
        }
    }
}