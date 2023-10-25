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

#ifndef qop_joins_hpp_
#define qop_joins_hpp_

#include "qop.hpp"

/**
 * cross_join_op implements a binary operator for constructing the cartesian
 * product of the results provided by the two input query operators.
 */
struct cross_join_op : public qop, public std::enable_shared_from_this<cross_join_op> {
  cross_join_op() : phases_(0) {}
  cross_join_op(qop_ptr &rhs) : rhs_(rhs) {}
  ~cross_join_op() = default;

  void dump(std::ostream &os) const override;

  void process_left(query_ctx &ctx, const qr_tuple &v);
  void process_right(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 1;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  bool is_binary() const override { return true; }

  qop_ptr &get_rhs() { return rhs_; }

private:
  std::size_t phases_;
  std::list<qr_tuple> input_;
  qop_ptr rhs_;
};

  /**
   * nested_loop_join_op implements a nested loop join operator for merging tuples of two
   * query pipelines if the node at a given position in the left tuple
   * is the same as the node at another given position in the right tuple.
   * The node positions are specified by the pos pair.
   */
struct nested_loop_join_op : public qop, public std::enable_shared_from_this<nested_loop_join_op> {
  nested_loop_join_op(std::pair<int, int> pos) : left_right_nodes_(pos) {} 
  nested_loop_join_op(std::pair<int, int> pos, qop_ptr &rhs) : left_right_nodes_(pos), rhs_(rhs) {}
  ~nested_loop_join_op() = default;

  void dump(std::ostream &os) const override;

  void process_left(query_ctx &ctx, const qr_tuple &v);
  void process_right(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 1;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);    
  }

  bool is_binary() const override { return true; }
  
  qop_ptr &get_rhs() { return rhs_; }

  std::pair<int, int> left_right_nodes_;
private:
  std::vector<qr_tuple> input_;
  std::vector<node::id_t> join_ids_;
  
  qop_ptr rhs_;
};

  /**
   * hash_join_op implements a hashjoin operator for merging tuples of two
   * query pipelines if the node at a given position in the left tuple
   * is the same as the node at another given position in the right tuple.
   * The node positions are specified by the pos pair.
   */
struct hash_join_op : public qop, public std::enable_shared_from_this<hash_join_op> {
  hash_join_op(std::pair<int, int> pos) : left_right_nodes_(pos) {} 
  hash_join_op(std::pair<int, int> pos, qop_ptr &rhs) : left_right_nodes_(pos), rhs_(rhs) {} 
  ~hash_join_op() = default;

  void dump(std::ostream &os) const override;

  void build_phase(query_ctx &ctx, const qr_tuple &v);
  void probe_phase(query_ctx &ctx, const qr_tuple &v);
  static uint64_t hasher(uint64_t id);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 1;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);    
  }

  bool is_binary() const override { return true; }

  qop_ptr &get_rhs() { return rhs_; }

  std::pair<int, int> left_right_nodes_;
private:
  const static int BUCKETS = 10;
  std::vector<qr_tuple> input_[BUCKETS];
  std::vector<node::id_t> join_ids_[BUCKETS];
  qop_ptr rhs_;
};

/**
 * left_outer_join_op implements a left outerjoin operator for merging tuples 
 * of two queries based on the given join condition. 
 * Dangling tuples are padded with "null_val" 
 */
struct left_outer_join_op : public qop, public std::enable_shared_from_this<left_outer_join_op> {
  left_outer_join_op(const expr &ex) : phases_(0), ex_(ex) {}
  left_outer_join_op(std::function<bool(const qr_tuple &, const qr_tuple &)> pred) : phases_(0), pred_(pred) {} 
  ~left_outer_join_op() = default;

  void dump(std::ostream &os) const override;

  void process_left(query_ctx &ctx, const qr_tuple &v);
  void process_right(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);
  
  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {}

  bool is_binary() const override { return true; }
  
  expr get_expression() { return ex_; }

private:
  std::size_t phases_;
  std::vector<qr_tuple> input_;
  std::function<bool(const qr_tuple &, const qr_tuple &)> pred_;
  expr ex_;  
};

#endif