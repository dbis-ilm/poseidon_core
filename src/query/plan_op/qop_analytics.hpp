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

#ifndef qop_analytics_hpp_
#define qop_analytics_hpp_

#include "qop.hpp"
#include "shortest_path.hpp"

/**
 * shortest_path_opr implements an operator that finds the
 * unweighted shortest path between two nodes.
 * all_spaths_ specfies if all shortest path of equal 
 * distance are searched.
 */
struct shortest_path_opr : public qop, public std::enable_shared_from_this<shortest_path_opr> {
  shortest_path_opr(std::pair<std::size_t, std::size_t> uv,
    rship_predicate pred, bool bidir, bool all) : bidirectional_(bidir),
          all_spaths_(all), rpred_(pred), start_stop_(uv) {}
  ~shortest_path_opr() = default;

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
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  path_item path_;
  bool bidirectional_;
  bool all_spaths_;
  rship_predicate rpred_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

/**
 * weighted_shortest_path_opr implements an operator that finds the
 * weighted shortest path between two nodes.
 * all_spaths_ specfies if all shortest path of equal 
 * weight are searched.
 */
struct weighted_shortest_path_opr : public qop, public std::enable_shared_from_this<weighted_shortest_path_opr> {
  weighted_shortest_path_opr(std::pair<std::size_t, std::size_t> uv, rship_predicate pred,
    rship_weight weight, bool bidir, bool all) : bidirectional_(bidir), all_spaths_(all),
      rpred_(pred), rweight_(weight), start_stop_(uv) {}
  ~weighted_shortest_path_opr() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  path_item path_;
  bool bidirectional_;
  bool all_spaths_;
  rship_predicate rpred_;
  rship_weight rweight_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

/**
 * k_weighted_shortest_path_opr implements an operator that finds the
 * top k weighted shortest path between two nodes.
 */
struct k_weighted_shortest_path_opr : public qop, public std::enable_shared_from_this<k_weighted_shortest_path_opr> {
  k_weighted_shortest_path_opr(std::pair<std::size_t, std::size_t> uv,
    std::size_t k, rship_predicate pred, rship_weight weight, bool b) : 
    k_(k), bidirectional_(b), rpred_(pred), rweight_(weight), start_stop_(uv) {}
  ~k_weighted_shortest_path_opr() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  std::size_t k_;
  path_item path_;
  bool bidirectional_;
  rship_predicate rpred_;
  rship_weight rweight_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

#endif