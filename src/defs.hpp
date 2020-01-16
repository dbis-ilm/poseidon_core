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

#ifndef defs_hpp_
#define defs_hpp_

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <boost/variant.hpp>

#define POSEIDON_VERSION "0.0.3"

/**
 * Typedef used for codes in string dictionaries and type tables.
 */
using dcode_t = uint32_t;

/**
 * Typedef used for offset in tables used as index. In a table the offset
 * starts always with 0.
 */
using offset_t = uint64_t;

/**
 * Typedef used for memory pointers.
 */
using ptr_t = uint8_t *;

/**
 *
 */
constexpr uint64_t UNKNOWN = std::numeric_limits<uint64_t>::max();

struct node;
struct relationship;

/**
 * Typedef for an element (node, relationship, value) that might be part of a
 * query result.
 */
using query_result =
    boost::variant<node *, relationship *, int, double, std::string>;

/**
 * Typedef for a list of result elements which are passed to the next query
 * operator in an execution plan.
 */
using qr_tuple = std::vector<query_result>;

#ifdef USE_PMDK

/**
 * Includes for PMDK.
 */
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

template <typename T> using p_ptr = pmem::obj::persistent_ptr<T>;

template <typename T> using p = pmem::obj::p<T>;

template <typename T, typename... Args>
inline p_ptr<T> p_make_ptr(Args &&... args) {
  return pmem::obj::make_persistent<T>(std::forward<Args>(args)...);
}

#else

template <typename T> using p_ptr = std::shared_ptr<T>;

template <typename T> using p = T;

template <typename T, typename... Args>
inline p_ptr<T> p_make_ptr(Args &&... args) {
  return std::make_shared<T>(std::forward<Args>(args)...);
}

#endif

#endif
