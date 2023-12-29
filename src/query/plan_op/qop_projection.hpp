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

#ifndef qop_projection_hpp_
#define qop_projection_hpp_

#include "qop.hpp"
#include "qop_builtins.hpp"

enum class prj {
  unknown,
  forward,         // just pass the value
  int_property,    // get an int property
  uint64_property, // get a uint64_t property
  string_property, // get a string property
  double_property, // get a string property
  ptime_property,  // get a ptime property
  date_property,   // get a date property
  int_property_as_datestring,
  int_property_as_datetimestring,
  label,           // get the label of a node or relationship
  function         // invoke a function
};


using builtin_func = std::function<query_result(query_ctx&, const query_result&)>;
using user_defined_func1 = std::function<query_result(query_ctx&, const query_result&)>;
using user_defined_func2 = std::function<query_result(query_ctx&, const query_result&, const query_result&)>;

using user_defined_func = boost::variant<user_defined_func1, user_defined_func2>;

/**
 * projection implements a project operator.
 */
struct projection : public qop, public std::enable_shared_from_this<projection> {
  struct expr {
    std::size_t idx;   // index of the input in the query_result
    std::string pname; // name of the property pr empty 
    prj pfunc;         // projection function
  };

  using expr_list = std::vector<expr>;

  struct prj_func {
    std::size_t idx;
    builtin_func func;
  };

  using prj_func_list = std::vector<prj_func>;

  using udf_list = std::vector<user_defined_func>;

  projection(const expr_list &exprs);
  projection(const expr_list &exprs, udf_list &ul);

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor& vis, unsigned& op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_ += next_offset, interpreted);      
  }

  void init_expressions();

  expr_list exprs_; // the list of defined projection expressions
  udf_list udfs_;

  prj_func_list pfuncs_;
  std::set<std::size_t> accessed_; // a set of variables referenced in the projection expression, e.g. 0, 1, 3....
};

#endif