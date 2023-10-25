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

#ifndef qop_aggregates_hpp_
#define qop_aggregates_hpp_

#include "qop.hpp"


/**
 * aggregate is a query operator for aggregating results. Multiple different aggregates are possible
 * but the operator produces only a single result tuple.
 */
struct aggregate : public qop, public std::enable_shared_from_this<aggregate> {
  /**
   * Aggregation expression.
   */
  struct expr {
    enum func_t { f_count, f_sum, f_min, f_max, f_avg, f_pcount } func; // agggregate function
    uint32_t var; // result variable (0, 1, ...)
    std::string property; // property name if result variable refers to a node or relationship, otherwise empty
    qr_type aggr_type; // typecode of aggregation - corresponds to query_result.which()

    // expression constructors
    expr(func_t f, uint32_t v, const std::string& p, qr_type t) : func(f), var(v), property(p), aggr_type(t) {}
    expr(uint32_t v, const std::string& p) : var(v), property(p) {}
  };

  /**
   * Constructor for aggregate operator.
  */
  aggregate(const std::vector<expr>& exp) : aggr_exprs_ (exp), aggr_vals_(exp.size()) { init_aggregates(); }

  /**
   * Destructor.
   */
  ~aggregate() = default;

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

  /**
   * Initialize the aggregates
   */
  void init_aggregates(); 

  // list of aggregate expressions
  std::vector<expr> aggr_exprs_;

  // typedef for aggregate values
  using val_t = boost::variant<
    double,                  // double values (for min, max, sum)
    int,                     // int values (for min, max, sum, count)
    uint64_t,                // uint64_t (for min, max, sum)
    std::string,             // string values (for min, max)  
    std::pair<double, int>>; // pair of values for avg (stores sum, cnt)
  // list of aggregate values computed using aggr_exprs_ from the input tuples
  std::vector<val_t> aggr_vals_;

  /**
   * Helper functions for getting values or property values from the input tuples.
   */
  static int get_int_value(query_ctx &ctx, const qr_tuple& v, const expr& ex);
  static double get_double_value(query_ctx &ctx, const qr_tuple& v, const expr& ex);
  static std::string get_string_value(query_ctx &ctx, const qr_tuple& v, const expr& ex);
  static uint64_t get_uint64_value(query_ctx &ctx, const qr_tuple& v, const expr& ex);
};

struct group_by : public qop, public std::enable_shared_from_this<group_by> {
  using expr = aggregate::expr;
  using val_t = aggregate::val_t;
  using aggr_vals_t = std::vector<val_t>;

  struct group {
    uint32_t var;
    std::string property;
    qr_type grp_type; // typecode of grouping - corresponds to query_result.which()
  };

  group_by(const std::vector<group>& grps, const std::vector<expr>& exp) : 
    groups_(grps), aggr_exprs_ (exp) {}
  ~group_by() = default;

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

  aggr_vals_t init_aggregates(); 
  void update_aggregates(query_ctx &ctx, aggr_vals_t& aval, const qr_tuple& v); 

  uint64_t hasher(query_ctx &ctx, const qr_tuple& v);

  std::vector<group> groups_;
  std::vector<expr> aggr_exprs_;

  std::unordered_map<uint64_t, qr_tuple> group_keys_;
  std::unordered_map<uint64_t, aggr_vals_t> aggr_vals_;
};

#endif