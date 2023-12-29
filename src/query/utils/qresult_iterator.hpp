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

#ifndef qresult_iterator_hpp_
#define qresult_iterator_hpp_

#include <list>
#include <memory>
#include <vector>
#include <set>
#include <iterator>
#include <condition_variable>
#include "defs.hpp"
#include "query_ctx.hpp"

struct node;
struct relationship;

struct null_t {
    explicit constexpr null_t(int) {}
  inline bool operator()(const null_t& one, const null_t& two) { return true; }
 inline bool operator==(const null_t& other) const { return true; }
};

inline constexpr null_t null_val(-1);

struct array_t {
  array_t(std::vector<uint64_t> v) : elems(v) {}
  inline bool operator==(const array_t& other) const { return elems == other.elems; }
  std::vector<uint64_t> elems;
};

/**
 * Typedef for an element (node, relationship, value) that might be part of a
 * query result. null_t is used to represent NULL values.
 */
using query_result =
    boost::variant<node *, relationship *, int, double, std::string, 
                    uint64_t, boost::posix_time::ptime, array_t, null_t, node_description, rship_description>;

#define qv_ query_result

inline node * qv_get_node(const query_result& v) { return boost::get<node*>(v); }
inline relationship * qv_get_relationship(const query_result& v) { return boost::get<relationship*>(v); }
inline int qv_get_int(const query_result& v) { return boost::get<int>(v); }
inline uint64_t qv_get_uint64(const query_result& v) { return boost::get<uint64_t>(v); }
inline double qv_get_double(const query_result& v) { return boost::get<double>(v); }
inline std::string qv_get_string(const query_result& v) { return boost::get<std::string>(v); }
inline const node_description& qv_get_node_descr(const query_result& v) { return boost::get<const node_description&>(v); }
inline const rship_description& qv_get_rship_descr(const query_result& v) { return boost::get<const rship_description&>(v); }

enum qr_type {
  node_ptr_type = 0,
  rship_ptr_type = 1,
  int_type = 2,
  double_type = 3,
  string_type = 4,
  uint64_type = 5,
  ptime_type = 6,
  array_type = 7,
  null_type = 8,
  node_descr_type = 9,
  rship_descr_type = 10
};

/**
 * Typedef for a list of result elements which are passed to the next query
 * operator in an execution plan.
 */
using qr_tuple = std::vector<query_result>;

/**
 * Create a query_result object from a property value.
 */
query_result qv_from_pitem(const p_item& p);

/**
 * result_set is used to collect query results.
 */
struct result_set {
//  using sort_spec = const std::vector<std::pair<std::size_t, bool>>; // std::vector<simple_proj_spec>
  
  struct sort_spec {
    enum sort_order { None = 0, Asc = 1, Desc = 2 } s_order; // sort order - used only for the Sort operator
    std::size_t vidx;  // index of the variable in result tuple 
    dcode_t pcode;     // dictionary code of the referenced property
    std::size_t cmp_type; // typecode of comparison - corresponds to query_result.which()

    sort_spec() = default;
    sort_spec(std::size_t idx, std::size_t cmp, sort_order so = Asc) : s_order(so), vidx(idx), pcode(UNKNOWN_CODE), cmp_type (cmp) {}
  };
  using sort_spec_list = std::vector<sort_spec>;
  using iterator = std::list<qr_tuple>::iterator;

  /**
   * Constructors.
   */
  result_set() = default;
  result_set(const result_set &rs) : data(rs.data) {}

    iterator begin() { return data.begin(); }
    iterator end() { return data.end(); }

  /**
   * Block the current thread until the result data is complete.
   */
  void wait();

  /**
   * Notify the waiting thread that the result is complete.
   */
  void notify();

  /**
   * Append the given element to the result set.
   */
  inline void append(const qr_tuple &elem) { data.push_back(elem); }

  /**
   * Sort the result by the given sort specification.
   */
  void sort(query_ctx& ctx, const sort_spec_list &spec);
  void sort(query_ctx& ctx, std::initializer_list<sort_spec> l);

  void sort(query_ctx& ctx, std::function<bool(const qr_tuple &, const qr_tuple &)> cmp);

  /**
   * Comparison operator.
   */
  bool operator==(const result_set &other) const;

  std::list<qr_tuple> data; // the result data

private:
  bool qr_compare(query_ctx& ctx, const qr_tuple &qr1, const qr_tuple &qr2,
                  const sort_spec_list &spec);

  // mutex and condition variable used to notify a waiting thread when the
  // result set is complete
  std::mutex m;
  std::condition_variable cond_var;
  std::atomic<bool> ready{false};
};


class qresult_iterator {
public:
    qresult_iterator(result_set&& rs) : rset_(rs) {}

    using iterator = std::list<qr_tuple>::iterator;

    iterator begin() { return rset_.begin(); }
    iterator end() { return rset_.end(); }

    void wait() { rset_.wait(); }

    const result_set& result() const { return rset_; }
    std::size_t result_size() const { return rset_.data.size(); }

    std::string to_string() const;

private:
    result_set rset_;
};

#endif
