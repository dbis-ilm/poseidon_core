/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef qop_algorithm_hpp_
#define qop_algorithm_hpp_

#include <vector>
#include <any>
#include "qop.hpp"

/**
 * algorithm_op is an operator for invoking graph algorithms by name. Two modes are supported:
 * - In TUPLE mode the algorithm is called for each input tuple. The result (a tuple) is appended to the input and forwarded
 *   to the subscriber. 
 * - In SET mode all the input tuples are collected and finally passed to the algorithm. The result is a single tuple.
*/
struct algorithm_op : public qop, public std::enable_shared_from_this<algorithm_op> {
    enum mode_t { m_tuple, m_set } call_mode_; // call mode of algorithm: per tuple or whole result set  
    using param_list = std::vector<std::any>; // parameter list for invoking the algorithm

    // function pointer type to a algorithm called in tuple mode
    using tuple_algorithm_func = std::function<qr_tuple(query_ctx&, const qr_tuple&, param_list&)>;
    // function pointer type to a algorithm called in set mode
    using set_algorithm_func = std::function<qr_tuple(query_ctx&, result_set&, param_list&)>;

    static void register_algorithm(const std::string& name, tuple_algorithm_func fptr) {
      tuple_algorithms_.insert({ name, fptr });
    }

    static void register_algorithm(const std::string& name, set_algorithm_func fptr) {
      set_algorithms_.insert({ name, fptr });
    }
   
    /**
     * Constructor for algorithm operator.
     */
    algorithm_op(const std::string& algo_name, mode_t m, param_list& args);

  /**
   * Destructor.
   */
  ~algorithm_op() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_ += next_offset, interpreted);
  }

    std::string algorithm_name_;          // name of algorithm
    param_list args_;                     // parameter list
    tuple_algorithm_func tuple_func_ptr_; // function pointer to a algorithm called in tuple mode (only one of both is used)
    set_algorithm_func set_func_ptr_;     // function pointer to a algorithm called in set mode
    result_set input_;                    // input data for algorithm (only for set mode)

    static std::map<std::string, tuple_algorithm_func> tuple_algorithms_;
    static std::map<std::string, set_algorithm_func> set_algorithms_;
};

#endif